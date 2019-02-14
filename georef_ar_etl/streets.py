from .etl import ETL
from .models import Province, Department, Street
from . import extractors, transformers, loaders, utils, constants, patch


class StreetsETL(ETL):
    def __init__(self):
        super().__init__(constants.STREETS)

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'streets_url')
        filename = extractors.download_url('calles.zip', url, ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.STREETS_RAW_TABLE,
                        geom_type='MultiLineString', encoding='latin1',
                        precision=True, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_streets = utils.automap_table(constants.STREETS_RAW_TABLE, ctx)

        # Aplicar parche (nuevo en ETL2)
        self._patch_raw_streets(raw_streets, ctx)

        # Leer la tabla raw_streets para crear las entidades procesadas
        self._insert_clean_streets(raw_streets, ctx)

    def _patch_raw_streets(self, raw_streets, ctx):
        def update_ushuaia(row):
            row.nomencla = '94015' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94015' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Ushuaia (agregado en ETL2)
        patch.apply_fn(raw_streets, update_ushuaia, ctx,
                       raw_streets.nomencla.like('94014%'))

        def update_rio_grande(row):
            row.nomencla = '94008' + row.nomencla[constants.DEPARTMENT_ID_LEN:]
            row.codloc = '94008' + row.codloc[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Río Grande (agregado en ETL2)
        patch.apply_fn(raw_streets, update_rio_grande, ctx,
                       raw_streets.nomencla.like('94007%'))

    def _insert_clean_streets(self, raw_streets, ctx):
        streets = []

        ctx.query(Street).delete()

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = ctx.query(raw_streets).\
            filter(raw_streets.tipo != 'OTRO').\
            yield_per(bulk_size)

        count = query.count()

        for raw_street in utils.pbar(query, ctx, total=count):
            street_id = raw_street.nomencla
            prov_id = street_id[:constants.PROVINCE_ID_LEN]
            dept_id = street_id[:constants.DEPARTMENT_ID_LEN]

            # TODO: Cachear estas cosas?
            province = ctx.query(Province).get(prov_id)
            department = ctx.query(Department).get(dept_id)

            street = Street(
                id=street_id,
                nombre=utils.clean_string(raw_street.nombre),
                categoria=utils.clean_string(raw_street.tipo),
                fuente=constants.STREETS_SOURCE,
                inicio_derecha=raw_street.desded or 0,
                fin_derecha=raw_street.hastad or 0,
                inicio_izquierda=raw_street.desdei or 0,
                fin_izquierda=raw_street.hastai or 0,
                geometria=raw_street.geom,
                provincia_id=province.id,
                departamento_id=department.id
            )

            assert len(street.id) == constants.STREET_ID_LEN

            streets.append(street)

            if len(streets) > bulk_size:
                ctx.session.add_all(streets)
                streets.clear()

        ctx.session.add_all(streets)
