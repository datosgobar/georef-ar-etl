import os
from zipfile import ZipFile
import tarfile


def extract_zipfile(filename, context):
    dirname = filename.split('.')[0]
    if context.cache.isdir(dirname):
        context.logger.info('Zip: Removiendo directorio "%s" anterior.',
                            dirname)
        context.cache.removetree(dirname)

    # TODO: Opcional: implementar caso donde no existe getsyspath()
    dirpath = os.path.dirname(context.cache.getsyspath(filename))

    with context.cache.open(filename, 'rb') as f:
        with ZipFile(f) as zipf:
            zipf.extractall(os.path.join(dirpath, dirname))

    return dirname


def extract_tarfile(filename, context):
    dirname = filename.split('.')[0]
    if context.cache.isdir(dirname):
        context.logger.info('Tar: Removiendo directorio "%s" anterior.',
                            dirname)
        context.cache.removetree(dirname)

    # TODO: Opcional: implementar caso donde no existe getsyspath()
    sys_filename = context.cache.getsyspath(filename)
    dirpath = os.path.dirname(sys_filename)

    with tarfile.open(sys_filename) as tarf:
        tarf.extractall(os.path.join(dirpath, dirname))

    return dirname
