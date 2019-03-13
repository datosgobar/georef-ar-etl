import os
import subprocess
import json
from datetime import datetime
from datetime import timezone
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from . import constants, utils
from .process import Step, ProcessException


class Ogr2ogrStep(Step):
    def __init__(self, table_name, geom_type, encoding, precision=True):
        super().__init__('ogr2ogr')
        self._table_name = table_name
        self._geom_type = geom_type
        self._encoding = encoding
        self._precision = precision

    def new_env(self, new_vars):
        env = os.environ.copy()
        for key, value in new_vars.items():
            env[key] = value

        return env

    def automap_table(self, ctx):
        metadata = MetaData()
        metadata.reflect(ctx.engine, only=[self._table_name])
        base = automap_base(metadata=metadata)
        base.prepare()

        return getattr(base.classes, self._table_name)

    def _run_internal(self, dirname, ctx):
        glob = ctx.fs.glob(os.path.join(dirname, '*.shp'))
        if glob.count().files != 1:
            raise ProcessException(
                'Se detectó más de un archivo .shp en el directorio.')

        shp = next(iter(glob))
        shp_path = ctx.fs.getsyspath(shp.path)

        ctx.report.info('Ejecutando ogr2ogr sobre %s.', shp_path)
        args = [
            'ogr2ogr', '-overwrite', '-f', 'PostgreSQL',
            ('PG:host={host} ' +
             'user={user} ' +
             'password={password} ' +
             'dbname={database}').format(**ctx.config['db']),
            '-nln', self._table_name,
            '-nlt', self._geom_type,
            '-lco', 'GEOMETRY_NAME=geom'
        ]

        if not self._precision:
            args.extend(['-lco', 'PRECISION=NO'])

        args.append(shp_path)
        result = subprocess.run(args, env=self.new_env({
            'SHAPE_ENCODING': self._encoding
        }))

        if result.returncode:
            raise ProcessException(
                'El comando ogr2ogr retornó codigo {}.'.format(
                    result.returncode))

        return self.automap_table(ctx)


class CreateJSONFileStep(Step):
    def __init__(self, table, filename):
        super().__init__('create_json_file', reads_input=False)
        self._table = table
        self._filename = filename

    def _run_internal(self, _, ctx):
        data = {}
        entities = []
        now = datetime.now(timezone.utc)
        bulk_size = ctx.config.getint('etl', 'bulk_size')

        data['fecha_creacion'] = str(now)
        data['timestamp'] = int(now.timestamp())
        data['version'] = constants.ETL_VERSION

        query = ctx.session.query(self._table).yield_per(bulk_size)
        count = query.count()
        cached_session = ctx.cached_session()

        ctx.report.info('Transformando entidades a JSON...')
        for entity in utils.pbar(query, ctx, total=count):
            entities.append(entity.to_dict(cached_session))

        data['datos'] = entities

        utils.ensure_dir(constants.ETL_VERSION, ctx)
        utils.ensure_dir(constants.LATEST_DIR, ctx)

        ctx.report.info('Escribiendo archivo JSON...')
        filepath = os.path.join(constants.ETL_VERSION, self._filename)
        with ctx.fs.open(filepath, 'w') as f:
            json.dump(data, f, ensure_ascii=False)

        ctx.report.info('Creando copia del archivo...')
        filepath_latest = os.path.join(constants.LATEST_DIR, self._filename)
        utils.copy_file(filepath, filepath_latest, ctx)
