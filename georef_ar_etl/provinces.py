from .etl import ETL
from . import extractors, transformers, loaders
from .models import Province, ProvinceRaw


class ProvincesETL(ETL):
    def __init__(self):
        super().__init__("Provincias")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'provinces_url')
        filename = extractors.download_url('provincias.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, context)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=ProvinceRaw.__tablename__,
                        geom_type='MULTIPOLYGON', encoding='UTF-8',
                        precision=True, context=context)

    def _transform_insert_provinces(self, raw_provinces, context):
        provinces = []

        for province in raw_provinces:
            centroid = context.session.scalar(province.geom.ST_Centroid())
            from geoalchemy2.shape import to_shape
            g = to_shape(centroid)
            print(g)
