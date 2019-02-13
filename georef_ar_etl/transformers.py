import os
from zipfile import ZipFile
import tarfile


def extract_zipfile(filename, context):
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


def extract_tarfile(filename, context):
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
