import os
import subprocess
import json
import shutil
from datetime import datetime
from datetime import timezone
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from . import constants, utils
from .process import Step, ProcessException

OGR2OGR_CMD = 'ogr2ogr'


class Ogr2ogrStep(Step):
    def __init__(self, table_name, geom_type, encoding, precision=True,
                 metadata=None, db_config=None):
        super().__init__('ogr2ogr')
        if not shutil.which(OGR2OGR_CMD):
            raise RuntimeError('ogr2ogr is not installed.')

        self._table_name = table_name
        self._geom_type = geom_type
        self._encoding = encoding
        self._precision = precision
        self._metadata = metadata or MetaData()
        self._db_config = db_config

    def new_env(self, new_vars):
        env = os.environ.copy()
        for key, value in new_vars.items():
            env[key] = value

        return env

    def automap_table(self, ctx):
        self._metadata.reflect(ctx.engine, only=[self._table_name])
        base = automap_base(metadata=self._metadata)
        base.prepare()

        return getattr(base.classes, self._table_name)

    def _run_internal(self, dirname, ctx):
        glob = ctx.fs.glob(os.path.join(dirname, '*.shp'))
        if glob.count().files != 1:
            raise ProcessException(
                'Se detectó más de un archivo .shp en el directorio.')

        shp = list(glob)[0]
        shp_path = ctx.fs.getsyspath(shp.path)
        db_config = self._db_config or ctx.config['db']

        ctx.report.info('Ejecutando ogr2ogr sobre %s.', shp_path)
        args = [
            OGR2OGR_CMD, '-overwrite', '-f', 'PostgreSQL',
            ('PG:host={host} ' +
             'user={user} ' +
             'password={password} ' +
             'dbname={database}').format(**db_config),
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
    def __init__(self, table, *filename_parts):
        super().__init__('create_json_file', reads_input=False)
        self._table = table
        self._filename = os.path.join(*filename_parts)

    def _run_internal(self, data, ctx):
        contents = {}
        entities = []
        now = datetime.now(timezone.utc)
        bulk_size = ctx.config.getint('etl', 'bulk_size')

        contents['fecha_creacion'] = str(now)
        contents['timestamp'] = int(now.timestamp())
        contents['version'] = constants.ETL_VERSION

        query = ctx.session.query(self._table).yield_per(bulk_size)
        count = query.count()
        cached_session = ctx.cached_session()

        ctx.report.info('Transformando entidades a JSON...')
        for entity in utils.pbar(query, ctx, total=count):
            entities.append(entity.to_dict(cached_session))

        contents['datos'] = entities
        dirname = os.path.dirname(self._filename)
        if dirname:
            utils.ensure_dir(dirname, ctx)

        ctx.report.info('Escribiendo archivo JSON...')
        with ctx.fs.open(self._filename, 'w') as f:
            json.dump(contents, f, ensure_ascii=False)

        return self._filename
