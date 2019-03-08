import os
import subprocess
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from . import utils
from .process import Step, ProcessException


def new_env(new_vars):
    env = os.environ.copy()
    for key, value in new_vars.items():
        env[key] = value

    return env


def automap_table(table_name, ctx):
    metadata = MetaData()
    metadata.reflect(ctx.engine, only=[table_name])
    base = automap_base(metadata=metadata)
    base.prepare()

    return getattr(base.classes, table_name)


class Ogr2ogrStep(Step):
    def __init__(self, table_name, geom_type, encoding, precision=True):
        super().__init__('ogr2ogr')
        self._table_name = table_name
        self._geom_type = geom_type
        self._encoding = encoding
        self._precision = precision

    def _run_internal(self, dirname, ctx):
        glob = ctx.cache.glob(os.path.join(dirname, '*.shp'))
        if glob.count().files != 1:
            raise ProcessException(
                'Se detectó más de un archivo .shp en el directorio.')

        shp = next(iter(glob))
        shp_path = ctx.cache.getsyspath(shp.path)

        ctx.logger.info('Ejecutando ogr2ogr sobre %s.', shp_path)
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
        result = subprocess.run(args, env=new_env({
            'SHAPE_ENCODING': self._encoding
        }))

        if result.returncode:
            raise ProcessException(
                'El comando ogr2ogr retornó codigo {}.'.format(
                    result.returncode))

        return automap_table(self._table_name, ctx)
