import os
from zipfile import ZipFile
import tarfile
from .process import Step


class ExtractZipStep(Step):
    def __init__(self):
        super().__init__('extract_zip')

    def _run_internal(self, filename, ctx):
        dirname = filename.split('.')[0]
        if ctx.cache.isdir(dirname):
            ctx.logger.info('Zip: Removiendo directorio "%s" anterior.',
                            dirname)
            ctx.cache.removetree(dirname)

        dirpath = os.path.dirname(ctx.cache.getsyspath(filename))

        with ctx.cache.open(filename, 'rb') as f:
            with ZipFile(f) as zipf:
                zipf.extractall(os.path.join(dirpath, dirname))

        return dirname


class ExtractTarStep(Step):
    def __init__(self):
        super().__init__('extract_tar')

    def _run_internal(self, filename, ctx):
        dirname = filename.split('.')[0]
        if ctx.cache.isdir(dirname):
            ctx.logger.info('Tar: Removiendo directorio "%s" anterior.',
                            dirname)
            ctx.cache.removetree(dirname)

        sys_filename = ctx.cache.getsyspath(filename)
        dirpath = os.path.dirname(sys_filename)

        with tarfile.open(sys_filename) as tarf:
            tarf.extractall(os.path.join(dirpath, dirname))

        return dirname
