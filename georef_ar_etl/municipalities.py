from .etl import ETL
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


class MunicipalitiesETL(ETL):
    def __init__(self):
        super().__init__("Municipios")

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'municipalities_url')
        filename = extractors.download_url('municipios.zip', url, ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.MUNICIPALITIES_RAW_TABLE,
                        geom_type='MultiPolygon', encoding='utf-8',
                        precision=True, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_municipalities = ctx.automap_table(
            constants.MUNICIPALITIES_RAW_TABLE)

        # Aplicar parche
        self._patch_raw_municipalities(raw_municipalities, ctx)

        # Leer la tabla raw_provinces para crear las entidades procesadas
        self._insert_clean_municipalities(raw_municipalities, ctx)

    def _patch_raw_municipalities(self, raw_municipalities, ctx):
        patch.delete(raw_municipalities, ctx, in1=None)
        patch.delete(raw_municipalities, ctx, gna=None)

        # TODO: Manejar mejor municipios con IDs inválidos
        patch.delete(raw_municipalities, ctx, in1='82210')
        patch.delete(raw_municipalities, ctx, in1='82287')
        patch.delete(raw_municipalities, ctx, in1='82119')

        patch.update_row_field(raw_municipalities, 'in1', '540287', ctx,
                               in1='550287')
        patch.update_row_field(raw_municipalities, 'in1', '540343', ctx,
                               in1='550343')
        patch.update_row_field(raw_municipalities, 'in1', '820277', ctx,
                               in1='800277')
        patch.update_row_field(raw_municipalities, 'in1', '585070', ctx,
                               in1='545070')
        patch.update_row_field(raw_municipalities, 'in1', '589999', ctx,
                               in1='549999')
        patch.update_row_field(raw_municipalities, 'in1', '629999', ctx,
                               in1='829999')

    def _insert_clean_municipalities(self, raw_municipalities, ctx):
        municipalities = []

        # TODO: Manejar comparación con municipios que ya están en la base
        ctx.query(Municipality).delete()
        query = ctx.query(raw_municipalities)
        count = query.count()

        ctx.logger.info('Insertando municipios procesados...')

        for raw_municipality in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid(raw_municipality, ctx)
            muni_id = raw_municipality.in1
            prov_id = muni_id[:constants.PROVINCE_ID_LEN]

            province = ctx.query(Province).get(prov_id)
            province_isct = geometry.get_intersection_percentage(
                province, raw_municipality, ctx, geom_field_a='geometria')

            municipality = Municipality(
                id=muni_id,
                nombre=utils.clean_string(raw_municipality.nam),
                nombre_completo=utils.clean_string(raw_municipality.fna),
                categoria=utils.clean_string(raw_municipality.gna),
                lon=lon, lat=lat,
                provincia_interseccion=province_isct,
                provincia_id=province.id,
                fuente=utils.clean_string(raw_municipality.sag),
                geometria=raw_municipality.geom
            )

            # TODO: Sistema que compruebe la integridad de los nuevos datos
            assert len(municipality.id) == constants.MUNICIPALITY_ID_LEN

            municipalities.append(municipality)

        ctx.session.add_all(municipalities)
