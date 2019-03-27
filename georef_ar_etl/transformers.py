import os
from zipfile import ZipFile, BadZipFile
import tarfile
from .exceptions import ValidationException, ProcessException
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

        try:
            with ctx.fs.open(filename, 'rb') as f:
                with ZipFile(f) as zipf:
                    zipf.extractall(os.path.join(dirpath, dirname))
        except BadZipFile as e:
            raise ProcessException(
                'No se pudo extraer el archivo .zip: {}'.format(e))

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

        try:
            with tarfile.open(sys_filename) as tarf:
                tarf.extractall(os.path.join(dirpath, dirname))
        except tarfile.ReadError as e:
            raise ProcessException(
                'No se pudo extraer el archivo .tar: {}'.format(e))

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
        count = query.count()
        cached_session = ctx.cached_session()
        deleted = []
        updated = set()
        added = set()
        errors = []

        if not count:
            raise ProcessException('No hay entidades a procesar.')

        ctx.report.info('Entidades a procesar: {}'.format(count))
        ctx.report.info('Insertando entidades procesadas...')

        for tmp_entity in utils.pbar(query, ctx, total=count):
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

        report_data = ctx.report.get_data(self.name)
        report_data['new_entities_ids'] = list(added)
        report_data['updated_entities_count'] = len(updated)
        report_data['deleted_entities_ids'] = deleted
        report_data['errors'] = errors

        return self._entity_class

    def _build_entities_query(self, tmp_entities, ctx):
        return ctx.session.query(tmp_entities)

    def _process_entity(self, entity, cached_session, ctx):
        raise NotImplementedError()

    def _patch_tmp_entities(self, tmp_entities, ctx):
        # Implementación default: noop
        pass
