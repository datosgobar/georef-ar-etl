from .etl import ETL
from . import extractors, transformers, loaders


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
        loaders.ogr2ogr(zip_dir, table_name='raw_municipios',
                        geom_type='MultiPolygon', encoding='utf-8',
                        precision=True, ctx=ctx)
