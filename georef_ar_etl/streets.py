from .etl import ETL
from . import extractors, transformers, loaders


class StreetsETL(ETL):
    def __init__(self):
        super().__init__("Calles")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'streets_url')
        filename = extractors.download_url('calles.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, context)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_calles',
                        geom_type='MULTILINESTRING', encoding='LATIN1',
                        precision=True, context=context)
