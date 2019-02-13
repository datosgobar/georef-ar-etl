from .etl import ETL
from . import extractors, transformers, loaders


class DepartmentsETL(ETL):
    def __init__(self):
        super().__init__("Departamentos")

    def _run_internal(self, context):
        # Descargar el archivo de la URL
        url = context.config.get('etl', 'departments_url')
        filename = extractors.download_url('departamentos.zip', url, context)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, context)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name='raw_departamentos',
                        geom_type='MULTIPOLYGON', encoding='UTF-8',
                        precision=True, context=context)
