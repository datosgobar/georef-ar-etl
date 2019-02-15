import os
import subprocess
from .etl import ProcessException


def new_env(new_vars):
    env = os.environ.copy()
    for key, value in new_vars.items():
        env[key] = value

    return env


def ogr2ogr(dirname, table_name, geom_type, encoding, precision, ctx):
    glob = ctx.cache.glob(os.path.join(dirname, '*.shp'))
    if glob.count().files != 1:
        raise ProcessException(
            'Se detectó más de un archivo .shp en el directorio especificado.')

    shp = next(iter(glob))
    shp_path = ctx.cache.getsyspath(shp.path)

    ctx.logger.info('Ejecutando ogr2ogr sobre %s.', shp_path)
    args = [
        'ogr2ogr', '-overwrite', '-f', 'PostgreSQL',
        ('PG:host={host} ' +
         'user={user} ' +
         'password={password} ' +
         'dbname={database}').format(**ctx.config['db']),
        '-nln', table_name,
        '-nlt', geom_type,
        '-lco', 'GEOMETRY_NAME=geom'
    ]

    if not precision:
        args.extend(['-lco', 'PRECISION=NO'])

    args.append(shp_path)
    result = subprocess.run(args, env=new_env({'SHAPE_ENCODING': encoding}))

    if result.returncode:
        raise ProcessException(
            'El comando ogr2ogr retornó codigo {}.'.format(result.returncode))
