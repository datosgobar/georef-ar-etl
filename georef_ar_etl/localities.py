from .etl import ETL
from . import extractors, transformers, loaders


class LocalitiesETL(ETL):
    def __init__(self):
        super().__init__("Localidades")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'localities_url')
        filename = extractors.download_url('localidades.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_tarfile(filename, context)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_localidades',
                        geom_type='MULTIPOINT', encoding='LATIN1',
                        precision=False, context=context)
