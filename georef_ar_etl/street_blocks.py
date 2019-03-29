import hashlib
from .process import Process, CompositeStep
from .models import Province, Department, Street, StreetBlock
from .exceptions import ValidationException, ProcessException
from .streets import update_commune_data
from . import extractors, loaders, utils, constants, patch, transformers


def create_process(config):
    url_template = config.get('etl', 'street_blocks_url_template')

    download_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.csv'.format(constants.STREET_BLOCKS, province_id),
            url_template.format(province_id),
            params={
                'CQL_FILTER': 'nomencla like \'{}%\''.format(province_id)
            }
        ) for province_id in constants.PROVINCE_IDS[:4]
    ])

    ogr2ogr_cstep = CompositeStep([
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString')
    ] + [
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString', overwrite=False)
    ] * (len(download_cstep) - 1))

    return Process(constants.STREET_BLOCKS, [
        utils.CheckDependenciesStep([Province, Department, Street]),
        download_cstep,
        ogr2ogr_cstep,
        utils.FirstResultStep,
        # utils.ValidateTableSchemaStep({
        #     'ogc_fid': 'integer',
        #     'nomencla': 'varchar',
        #     'codigo': 'double',
        #     'tipo': 'varchar',
        #     'nombre': 'varchar',
        #     'desdei': 'double',
        #     'desded': 'double',
        #     'hastai': 'double',
        #     'hastad': 'double',
        #     'codloc': 'varchar',
        #     'codaglo': 'varchar',
        #     'link': 'varchar',
        #     'geom': 'geometry'
        # }),
        StreetBlocksExtractionStep(),
    ])


class StreetBlocksExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('street_blocks_extraction', StreetBlock,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='ogc_fid')

    def _patch_tmp_entities(self, tmp_blocks, ctx):
        patch.apply_fn(tmp_blocks, update_commune_data, ctx,
                       tmp_blocks.nomencla.like('02%'))

        def update_ushuaia(row):
            row.nomencla = '94015' + row.nomencla[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Ushuaia (agregado en ETL2)
        patch.apply_fn(tmp_blocks, update_ushuaia, ctx,
                       tmp_blocks.nomencla.like('94014%'))

        def update_rio_grande(row):
            row.nomencla = '94008' + row.nomencla[constants.DEPARTMENT_ID_LEN:]

        # Actualizar calles de Río Grande (agregado en ETL2)
        patch.apply_fn(tmp_blocks, update_rio_grande, ctx,
                       tmp_blocks.nomencla.like('94007%'))

    def _run_internal(self, tmp_entities, ctx):
        self._patch_tmp_entities(tmp_entities, ctx)
        ctx.session.query(StreetBlock).delete()

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = ctx.session.query(tmp_entities).\
            filter(tmp_entities.tipo != 'OTRO').\
            yield_per(bulk_size)
        count = query.count()
        entities = []
        cached_session = ctx.cached_session()
        errors = []

        ctx.report.info('{} cuadras a procesar.'.format(count))
        ctx.report.info('Procesando cuadras...')

        for tmp_entity in utils.pbar(query, ctx, total=count):
            try:
                entity = self._process_entity(tmp_entity, cached_session, ctx)
                entities.append(entity)
            except ValidationException:
                errors.append(tmp_entity.nomencla)

            if len(entities) > bulk_size:
                ctx.session.add_all(entities)
                entities.clear()

        ctx.report.info('Cuadras procesadas.')
        ctx.report.info('Errores: {}'.format(len(errors)))

        ctx.session.add_all(entities)

    def _process_entity(self, tmp_block, cached_session, ctx):
        ogc_fid = str(tmp_block.ogc_fid).rjust(5, '0')
        block_id = tmp_block.nomencla + ogc_fid[-5:]
        street = cached_session.query(Street).get(tmp_block.nomencla)
        if not street:
            report_data = ctx.report.get_data(self.name)
            invalid_block_streets_ids = report_data.setdefault(
                'invalid_block_streets_ids', [])

            invalid_block_streets_ids.append(tmp_block.nomencla)

            raise ValidationException('No existe la calle con ID {}.'.format(
                tmp_block.nomencla))

        return StreetBlock(
            id=block_id,
            calle_id=street.id,
            inicio_derecha=tmp_block.desded or 0,
            fin_derecha=tmp_block.hastad or 0,
            inicio_izquierda=tmp_block.desdei or 0,
            fin_izquierda=tmp_block.hastai or 0,
            geometria=tmp_block.geom
        )
