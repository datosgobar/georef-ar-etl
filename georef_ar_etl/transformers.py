import os
from zipfile import ZipFile
import tarfile
from .process import Step
from . import utils


class ExtractZipStep(Step):
    def __init__(self):
        super().__init__('extract_zip')

    def _run_internal(self, filename, ctx):
        dirname = filename.split('.')[0]
        if ctx.fs.isdir(dirname):
            ctx.report.info('Zip: Removiendo directorio "%s" anterior.',
                            dirname)
            ctx.fs.removetree(dirname)

        dirpath = os.path.dirname(ctx.fs.getsyspath(filename))

        with ctx.fs.open(filename, 'rb') as f:
            with ZipFile(f) as zipf:
                zipf.extractall(os.path.join(dirpath, dirname))

        return dirname


class ExtractTarStep(Step):
    def __init__(self):
        super().__init__('extract_tar')

    def _run_internal(self, filename, ctx):
        dirname = filename.split('.')[0]
        if ctx.fs.isdir(dirname):
            ctx.report.info('Tar: Removiendo directorio "%s" anterior.',
                            dirname)
            ctx.fs.removetree(dirname)

        sys_filename = ctx.fs.getsyspath(filename)
        dirpath = os.path.dirname(sys_filename)

        with tarfile.open(sys_filename) as tarf:
            tarf.extractall(os.path.join(dirpath, dirname))

        return dirname


class EntitiesExtractionStep(Step):
    def __init__(self, name, entity_class):
        super().__init__(name)
        self._entity_class = entity_class

    def _run_internal(self, raw_entities, ctx):
        self._patch_raw_entities(raw_entities, ctx)

        # TODO: Manejar comparación con entidades que ya están en la base
        ctx.session.query(self._entity_class).delete()

        entities = []
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = self._build_entities_query(raw_entities, ctx).yield_per(
            bulk_size)
        count = query.count()
        cached_session = ctx.cached_session()

        ctx.report.info('Insertando entidades procesadas...')

        for raw_entity in utils.pbar(query, ctx, total=count):
            new_entity = self._process_entity(raw_entity, cached_session, ctx)
            entities.append(new_entity)

            if len(entities) > bulk_size:
                ctx.session.add_all(entities)
                entities.clear()

        ctx.session.add_all(entities)
        return self._entity_class

    def _build_entities_query(self, raw_entities, ctx):
        return ctx.session.query(raw_entities)

    def _process_entity(self, entity, cached_session, ctx):
        raise NotImplementedError()

    def _patch_raw_entities(self, raw_entities, ctx):
        # Implementación default: noop
        pass
