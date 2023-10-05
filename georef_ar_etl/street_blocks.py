from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, Step, CompositeStep
from .models import Street, StreetBlock
from . import utils, constants, loaders


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    def fetch_tmp_blocks_table(_, ctx):
        return utils.automap_table(constants.STREET_BLOCKS_TMP_TABLE, ctx)

    return Process(constants.STREET_BLOCKS, [
        utils.CheckDependenciesStep([Street,
                                     constants.STREET_BLOCKS_TMP_TABLE]),
        utils.FunctionStep(ctx_fn=fetch_tmp_blocks_table,
                           name='fetch_tmp_blocks_table',
                           reads_input=False),
        CompositeStep([
            StreetBlocksExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'street_blocks_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            StreetBlock, 'street_blocks', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('street_blocks', config),
    ])


class StreetBlocksExtractionStep(Step):
    def __init__(self):
        super().__init__('street_blocks_extraction')

    def _run_internal(self, tmp_blocks, ctx):
        ctx.session.query(StreetBlock).delete()

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        query = ctx.session.query(tmp_blocks, Street).\
            filter(tmp_blocks.tipo != constants.STREET_TYPE_OTHER).\
            join(Street, tmp_blocks.nomencla == Street.id).\
            yield_per(bulk_size)
        count = query.count()

        ctx.report.info('{} cuadras a procesar.'.format(count))
        ctx.report.info('Procesando cuadras...')

        for tmp_block, street in utils.pbar(query, ctx, total=count):
            block = self._process_block(tmp_block, street)
            utils.add_maybe_flush(block, ctx, bulk_size)

        ctx.report.info('Cuadras procesadas.')
        return StreetBlock

    def _process_block(self, tmp_block, street):
        # TODO: Revisar la forma de generar el valor block_id para garantizar la unicidad
        ogc_fid = str(tmp_block.ogc_fid).rjust(6, '0')
        block_id = tmp_block.nomencla + ogc_fid[-6:]

        return StreetBlock(
            id=block_id,
            calle_id=street.id,
            inicio_derecha=tmp_block.desded or 0,
            fin_derecha=tmp_block.hastad or 0,
            inicio_izquierda=tmp_block.desdei or 0,
            fin_izquierda=tmp_block.hastai or 0,
            geometria=tmp_block.geom
        )
