import os
from zipfile import ZipFile
import tarfile
from .exceptions import ValidationException
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
    def __init__(self, name, entity_class, entity_class_pkey,
                 tmp_entity_class_pkey):
        super().__init__(name)
        self._entity_class = entity_class
        self._entity_class_pkey = entity_class_pkey
        self._tmp_entity_class_pkey = tmp_entity_class_pkey

    def _run_internal(self, tmp_entities, ctx):
        self._patch_tmp_entities(tmp_entities, ctx)

        entities = []
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = self._build_entities_query(tmp_entities, ctx).yield_per(
            bulk_size)
        cached_session = ctx.cached_session()
        deleted = []
        updated = set()
        added = set()
        errors = []

        ctx.report.info('Insertando entidades procesadas...')

        for tmp_entity in utils.pbar(query, ctx, total=query.count()):
            try:
                new_entity = self._process_entity(tmp_entity, cached_session,
                                                  ctx)
            except ValidationException as e:
                errors.append(
                    (getattr(tmp_entity, self._tmp_entity_class_pkey), str(e))
                )
                continue

            new_entity_id = getattr(new_entity, self._entity_class_pkey)
            found = ctx.session.query(self._entity_class).\
                filter(getattr(self._entity_class, self._entity_class_pkey) ==
                       new_entity_id).\
                delete()

            if found:
                updated.add(new_entity_id)
            else:
                added.add(new_entity_id)

            entities.append(new_entity)

            if len(entities) > bulk_size:
                ctx.session.add_all(entities)
                entities.clear()

        ctx.session.add_all(entities)

        ctx.report.info('Buscando entidades eliminadas...')

        query = ctx.session.query(self._entity_class).yield_per(bulk_size)
        for entity in utils.pbar(query, ctx, total=query.count()):
            entity_id = getattr(entity, self._entity_class_pkey)

            if entity_id not in updated and entity_id not in added:
                ctx.session.query(self._entity_class).\
                    filter(getattr(self._entity_class,
                                   self._entity_class_pkey) == entity_id).\
                    delete()
                deleted.append(entity_id)

        ctx.report.info('Entidades nuevas: %s', len(added))
        ctx.report.info('Entidades actualizadas: %s', len(updated))
        ctx.report.info('Entidades eliminadas: %s', len(deleted))
        ctx.report.info('Errores: %s\n', len(errors))

        ctx.report.add_data(self.name, 'new_entities_ids', list(added))
        ctx.report.add_data(self.name, 'updated_entities_count', len(updated))
        ctx.report.add_data(self.name, 'deleted_entities_ids', deleted)
        ctx.report.add_data(self.name, 'errors', errors)

        return self._entity_class

    def _build_entities_query(self, tmp_entities, ctx):
        return ctx.session.query(tmp_entities)

    def _process_entity(self, entity, cached_session, ctx):
        raise NotImplementedError()

    def _patch_tmp_entities(self, tmp_entities, ctx):
        # Implementación default: noop
        pass
