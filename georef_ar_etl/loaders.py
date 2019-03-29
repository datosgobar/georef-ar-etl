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
    def __init__(self, table_name, geom_type, env=None, precision=True,
                 overwrite=True, metadata=None, db_config=None):
        super().__init__('ogr2ogr')
        if not shutil.which(OGR2OGR_CMD):
            raise RuntimeError('ogr2ogr is not installed.')

        self._table_name = table_name
        self._geom_type = geom_type
        self._env = env or {}
        self._precision = precision
        self._overwrite = overwrite
        self._metadata = metadata or MetaData()
        self._db_config = db_config

    def _new_env(self):
        env = os.environ.copy()
        for key, value in self._env.items():
            env[key] = value

        return env

    def _automap_table(self, ctx):
        self._metadata.reflect(ctx.engine, only=[self._table_name])
        base = automap_base(metadata=self._metadata)
        base.prepare()

        return getattr(base.classes, self._table_name)

    def _run_internal(self, filename, ctx):
        filepath = ctx.fs.getsyspath(filename)
        db_config = self._db_config or ctx.config['db']

        ctx.report.info('Ejecutando ogr2ogr sobre %s.', filepath)
        args = [
            OGR2OGR_CMD, '-f', 'PostgreSQL',
            ('PG:host={host} ' +
             'user={user} ' +
             'password={password} ' +
             'dbname={database}').format(**db_config),
            '-nln', self._table_name,
            '-nlt', self._geom_type,
            '-lco', 'GEOMETRY_NAME=geom'
        ]

        if os.path.splitext(filename)[1] == '.csv':
            args.extend([
                '-oo', 'KEEP_GEOM_COLUMNS=no',
                '-oo', 'GEOM_POSSIBLE_NAMES=geom',
            ])

        if self._overwrite:
            args.append('-overwrite')

        if not self._precision:
            args.extend(['-lco', 'PRECISION=NO'])

        args.append(filepath)
        result = subprocess.run(args, env=self._new_env())

        if result.returncode:
            raise ProcessException(
                'El comando ogr2ogr retornó codigo {}.'.format(
                    result.returncode))

        return self._automap_table(ctx)

    @property
    def overwrite(self):
        return self._overwrite

    @overwrite.setter
    def overwrite(self, val):
        self._overwrite = val


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
