from .etl import ETL
from .models import Province
from . import extractors, transformers, loaders, geometry, utils, constants


class ProvincesETL(ETL):
    def __init__(self):
        super().__init__(constants.PROVINCES, dependencies=[])

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'provinces_url')
        filename = extractors.download_url(constants.PROVINCES + '.zip', url,
                                           ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.PROVINCES_RAW_TABLE,
                        geom_type='MultiPolygon', encoding='utf-8',
                        precision=True, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_provinces = utils.automap_table(constants.PROVINCES_RAW_TABLE, ctx)

        # Leer la tabla raw_provinces para crear las provincias procesadas
        self._insert_clean_provinces(raw_provinces, ctx)

    def _insert_clean_provinces(self, raw_provinces, ctx):
        provinces = []
        iso_csv = utils.load_data_csv('iso-3166-provincias-arg.csv', ctx)
        iso_data = {row['id']: row for row in iso_csv}

        # TODO: Manejar comparación con provincias que ya están en la base
        ctx.query(Province).delete()
        query = ctx.query(raw_provinces)
        count = query.count()

        ctx.logger.info('Insertando provincias procesadas...')

        for raw_province in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid(raw_province, ctx)
            prov_id = raw_province.in1

            province = Province(
                id=prov_id,
                nombre=utils.clean_string(raw_province.nam),
                nombre_completo=utils.clean_string(raw_province.fna),
                iso_id=iso_data[prov_id]['3166-2 code'],
                iso_nombre=iso_data[prov_id]['subdivision name'],
                categoria=utils.clean_string(raw_province.gna),
                lon=lon, lat=lat,
                fuente=utils.clean_string(raw_province.sag),
                geometria=raw_province.geom
            )

            # TODO: Sistema que compruebe la integridad de los nuevos datos
            assert len(province.id) == constants.PROVINCE_ID_LEN

            provinces.append(province)

        ctx.session.add_all(provinces)
