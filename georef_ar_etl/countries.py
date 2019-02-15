from .etl import ETL
from . import extractors, transformers, loaders


class CountriesETL(ETL):
    def __init__(self):
        super().__init__("Países", dependencies=[])

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'countries_url')
        filename = extractors.download_url('paises.zip', url, ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_paises',
                        geom_type='MultiPolygon', encoding='latin1',
                        precision=True, ctx=ctx)
