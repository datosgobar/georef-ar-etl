import os
from zipfile import ZipFile
import tarfile


def extract_zipfile(filename, ctx):
    dirname = filename.split('.')[0]
    if ctx.cache.isdir(dirname):
        ctx.logger.info('Zip: Removiendo directorio "%s" anterior.', dirname)
        ctx.cache.removetree(dirname)

    dirpath = os.path.dirname(ctx.cache.getsyspath(filename))

    with ctx.cache.open(filename, 'rb') as f:
        with ZipFile(f) as zipf:
            zipf.extractall(os.path.join(dirpath, dirname))

    return dirname


def extract_tarfile(filename, ctx):
    dirname = filename.split('.')[0]
    if ctx.cache.isdir(dirname):
        ctx.logger.info('Tar: Removiendo directorio "%s" anterior.', dirname)
        ctx.cache.removetree(dirname)

    sys_filename = ctx.cache.getsyspath(filename)
    dirpath = os.path.dirname(sys_filename)

    with tarfile.open(sys_filename) as tarf:
        tarf.extractall(os.path.join(dirpath, dirname))

    return dirname
