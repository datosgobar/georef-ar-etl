import os
import subprocess
from zipfile import ZipFile
import tarfile

RAW_TABLE_NAME = 'raw_{}'


def transform_zipfile(filename, context):
    dirname = filename.split('.')[0]
    if context.fs.isdir(dirname):
        context.logger.info('Zip: Removiendo directorio "%s" anterior.',
                            dirname)
        context.fs.removetree(dirname)

    # TODO: Opcional: implementar caso donde no existe getsyspath()
    dirpath = os.path.dirname(context.fs.getsyspath(filename))

    with context.fs.open(filename, 'rb') as f:
        with ZipFile(f) as zipf:
            zipf.extractall(os.path.join(dirpath, dirname))

    return dirname


def transform_tarfile(filename, context):
    dirname = filename.split('.')[0]
    if context.fs.isdir(dirname):
        context.logger.info('Tar: Removiendo directorio "%s" anterior.',
                            dirname)
        context.fs.removetree(dirname)

    # TODO: Opcional: implementar caso donde no existe getsyspath()
    sys_filename = context.fs.getsyspath(filename)
    dirpath = os.path.dirname(sys_filename)

    with tarfile.open(sys_filename) as tarf:
        tarf.extractall(os.path.join(dirpath, dirname))

    return dirname


def new_env(new_vars):
    env = os.environ.copy()
    for key, value in new_vars.items():
        env[key] = value

    return env


def transform_ogr2ogr(geom_type, precision, dirname, context):
    glob = context.fs.glob(os.path.join(dirname, '*.shp'))
    if glob.count().files != 1:
        # TODO: Cambiar tipo de error
        raise RuntimeError('SHP count != 1')

    shp = next(iter(glob))
    # TODO: Opcional: implementar caso donde no existe getsyspath()
    shp_path = context.fs.getsyspath(shp.path)
    table_name = RAW_TABLE_NAME.format(dirname)

    context.logger.info('Ejecutando ogr2ogr sobre %s.', shp_path)
    args = [
        'ogr2ogr', '-overwrite', '-f', 'PostgreSQL',
        ('PG:host={host} ' +
         'user={user} ' +
         'password={password} ' +
         'dbname={database}').format(**context.config['db']),
        '-nln', table_name,
        '-nlt', geom_type,
        '-lco', 'GEOMETRY_NAME=geom'
    ]

    if not precision:
        args.extend(['-lco', 'PRECISION=NO'])

    args.append(shp_path)
    result = subprocess.run(args, env=new_env({'SHAPE_ENCODING': 'LATIN1'}))

    if result.returncode:
        # TODO: Cambiar tipo de errorp
        raise RuntimeError('ogr2ogr failed')

    return table_name
