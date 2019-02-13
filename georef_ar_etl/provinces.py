from .etl import ETL
from .constants import RAW_TABLE_NAME
from .models import Province
from . import extractors, transformers, loaders, geometry, utils


class ProvincesETL(ETL):
    def __init__(self):
        super().__init__("Provincias")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'provinces_url')
        filename = extractors.download_url('provincias.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, context)

        raw_provinces_table_name = RAW_TABLE_NAME.format('provincias')
        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=raw_provinces_table_name,
                        geom_type='MultiPolygon', encoding='utf-8',
                        precision=True, context=context)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_provinces = context.automap_table(raw_provinces_table_name)

        # Leer la tabla raw_provinces para crear las provincias procesadas
        self._insert_clean_provinces(raw_provinces, context)

    def _insert_clean_provinces(self, raw_provinces, context):
        provinces = []
        iso_csv = utils.load_data_csv('iso-3166-provincias-arg.csv', context)
        iso_data = {row['id']: row for row in iso_csv}

        # TODO: Manejar comparación con provincias que ya están en la base
        context.session.query(Province).delete()
        query = context.session.query(raw_provinces)
        count = query.count()

        context.logger.info('Insertando provincias procesadas...')

        for raw_province in utils.pbar(query, context, total=count):
            lon, lat = geometry.get_centroid(raw_province, context)
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
            assert len(province.id) == 2

            provinces.append(province)

        context.session.add_all(provinces)
