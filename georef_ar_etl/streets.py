from .etl import ETL
from .models import Province, Department, Street
from . import extractors, transformers, loaders, utils, constants
from . import patch


class StreetsETL(ETL):
    def __init__(self):
        super().__init__(constants.STREETS)

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'streets_url')
        filename = extractors.download_url('calles.zip', url, ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.STREETS_RAW_TABLE,
                        geom_type='MultiLineString', encoding='latin1',
                        precision=True, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_streets = utils.automap_table(constants.STREETS_RAW_TABLE, ctx)

        # Leer la tabla raw_streets para crear las entidades procesadas
        self._insert_clean_streets(raw_streets, ctx)

    def _insert_clean_streets(self, raw_streets, ctx):
        streets = []

        ctx.query(Street).delete()

        query = ctx.query(raw_streets)
        count = query.count()
        for raw_street in utils.pbar(query, ctx, total=count):
            pass

        ctx.session.add_all(streets)
