from .etl import ETL
from .models import Province, Department, Municipality, Locality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


class LocalitiesETL(ETL):
    def __init__(self):
        super().__init__(constants.LOCALITIES, [Province, Department,
                                                Municipality])

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'localities_url')
        filename = extractors.download_url('bahra.zip', url, ctx)

        # Descomprimir el .tar.gz
        zip_dir = transformers.extract_tarfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.LOCALITIES_RAW_TABLE,
                        geom_type='MultiPoint', encoding='latin1',
                        precision=False, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_localities = utils.automap_table(
            constants.LOCALITIES_RAW_TABLE, ctx)

        # Aplicar parche
        self._patch_raw_localities(raw_localities, ctx)

        # Leer la tabla raw_localities para crear las entidades procesadas
        self._insert_clean_localities(raw_localities, ctx)

        # Borrar la tabla temporal
        utils.drop_table(raw_localities, ctx)

    def _patch_raw_localities(self, raw_localities, ctx):
        # Agregado en ETL2
        patch.delete(raw_localities, ctx, cod_bahra='02000010000')

        # Actualizar códigos de comunas (departamentos)
        def update_commune_data(row):
            dept_id = int(row.cod_depto)
            row.cod_depto = str(dept_id * 7).rjust(len(row.cod_depto), '0')
            row.cod_bahra = (row.cod_prov + row.cod_depto + row.cod_loc +
                             row.cod_entida)

        patch.apply_fn(raw_localities, update_commune_data, ctx, cod_prov='02')

        # Borrar entidades sin ID
        patch.delete(raw_localities, ctx, cod_bahra=None)

        # Borrar 'EL FICAL'
        patch.delete(raw_localities, ctx, cod_bahra='70056060001',
                     nombre_bah='EL FICAL')

        # Actualiza códigos para los asentamientos del departamento de Río
        # Grande
        def update_rio_grande(row):
            row.cod_depto = '008'
            row.cod_bahra = (row.cod_prov + row.cod_depto + row.cod_loc +
                             row.cod_entida)
        patch.apply_fn(raw_localities, update_rio_grande, ctx, cod_prov='94',
                       cod_depto='007')

        # Actualiza códigos para los asentamientos del departamento de Usuhaia
        def update_ushuaia(row):
            row.cod_depto = '015'
            row.cod_bahra = (row.cod_prov + row.cod_depto + row.cod_loc +
                             row.cod_entida)
        patch.apply_fn(raw_localities, update_ushuaia, ctx, cod_prov='94',
                       cod_depto='014')

    def _insert_clean_localities(self, raw_localities, ctx):
        localities = []

        # TODO: Manejar comparación con municipios que ya están en la base
        ctx.query(Locality).delete()

        # Seleccionar un subconjunto de BAHRA
        query = ctx.query(raw_localities).filter(raw_localities.tipo_bahra.in_(
            constants.BAHRA_TYPES.keys()
        ))
        count = query.count()

        ctx.logger.info('Insertando localidades procesadas...')

        for raw_locality in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid_coordinates(raw_locality.geom,
                                                         ctx)
            loc_id = raw_locality.cod_bahra
            prov_id = loc_id[:constants.PROVINCE_ID_LEN]
            dept_id = loc_id[:constants.DEPARTMENT_ID_LEN]

            province = ctx.query(Province).get(prov_id)
            department = ctx.query(Department).get(dept_id)
            municipality = geometry.get_entity_at_point(Municipality,
                                                        raw_locality.geom, ctx)

            locality = Locality(
                id=loc_id,
                nombre=utils.clean_string(raw_locality.nombre_bah),
                categoria=utils.clean_string(raw_locality.tipo_bahra),
                lon=lon, lat=lat,
                provincia_id=province.id,
                departamento_id=department.id,
                municipio_id=municipality.id if municipality else None,
                fuente=utils.clean_string(raw_locality.fuente_ubi),
                geometria=raw_locality.geom
            )

            localities.append(locality)

        ctx.session.add_all(localities)
