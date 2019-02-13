from .etl import ETL
from . import extractors, transformers, loaders


class LocalitiesETL(ETL):
    def __init__(self):
        super().__init__("Localidades")

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'localities_url')
        filename = extractors.download_url('localidades.zip', url, ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_tarfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_localidades',
                        geom_type='MultiPoint', encoding='latin1',
                        precision=False, ctx=ctx)
