from .etl import ETL
from . import extractors, transformers, loaders


class CountriesETL(ETL):
    def __init__(self):
        super().__init__("Provincias")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'countries_url')
        filename = extractors.download_url('paises.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, context)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_paises',
                        geom_type='MultiPolygon', encoding='latin1',
                        precision=True, context=context)
