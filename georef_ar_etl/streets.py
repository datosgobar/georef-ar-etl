from sqlalchemy.sql import select, func
from sqlalchemy.sql.sqltypes import Integer
from .exceptions import ValidationException, ProcessException
from .process import Process, CompositeStep, Step
from .models import Province, Department, Street
from . import extractors, loaders, utils, constants, patch


def create_process(config):
    url_template = config.get('etl', 'street_blocks_url_template')

    download_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.csv'.format(constants.STREET_BLOCKS, province_id),
            url_template.format(province_id),
            params={
                'CQL_FILTER': 'nomencla like \'{}%\''.format(province_id)
            }
        ) for province_id in constants.PROVINCE_IDS
    ], name='download_cstep')

    ogr2ogr_cstep = CompositeStep([
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString')
    ] + [
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString', overwrite=False)
    ] * (len(download_cstep) - 1), name='ogr2ogr_cstep')

    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department]),
        download_cstep,
        ogr2ogr_cstep,
        utils.FirstResultStep,
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fid': 'varchar',
            'fnode_': 'varchar',
            'tnode_': 'varchar',
            'lpoly_': 'varchar',
            'rpoly_': 'varchar',
            'length': 'varchar',
            'codigo10': 'varchar',
            'nomencla': 'varchar',
            'codigo20': 'varchar',
            'ancho': 'varchar',
            'anchomed': 'varchar',
            'tipo': 'varchar',
            'nombre': 'varchar',
            'ladoi': 'varchar',
            'ladod': 'varchar',
            'desdei': 'varchar',
            'desded': 'varchar',
            'hastad': 'varchar',
            'hastai': 'varchar',
            'mzai': 'varchar',
            'mzad': 'varchar',
            'codloc20': 'varchar',
            'nomencla10': 'varchar',
            'nomenclai': 'varchar',
            'nomenclad': 'varchar',
            'geom': 'geometry'
        }),
        StreetsExtractionStep(),
        utils.ValidateTableSizeStep(size=151000, tolerance=1000),
        loaders.CreateJSONFileStep(Street, constants.ETL_VERSION,
                                   constants.STREETS + '.json'),
        utils.CopyFileStep(constants.LATEST_DIR,
                           constants.STREETS + '.json')
    ])


def update_commune_data(row):
    # De XX014XXXXXXXX pasar a XX002XXXXXXXX (dividir por 7)
    # Ver comentario en constants.py.
    prov_id_part = row.nomencla[:constants.PROVINCE_ID_LEN]
    dept_id_part = row.nomencla[constants.PROVINCE_ID_LEN:
                                constants.DEPARTMENT_ID_LEN]
    id_rest = row.nomencla[constants.DEPARTMENT_ID_LEN:]

    dept_id_int = int(dept_id_part)
    if dept_id_int % constants.CABA_DIV_FACTOR:
        # Alguno de los IDs no es divisible por el factor de división
        raise ProcessException(
            'El ID de comuna {} no es divisible por {}.'.format(
                dept_id_part,
                constants.CABA_DIV_FACTOR))

    dept_new_id_int = dept_id_int // constants.CABA_DIV_FACTOR
    row.nomencla = prov_id_part + str(dept_new_id_int).rjust(
        len(dept_id_part), '0') + id_rest


class StreetsExtractionStep(Step):
    def __init__(self):
        super().__init__('streets_extraction')

    def _patch_tmp_blocks(self, tmp_blocks, ctx):
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

        ctx.session.commit()

    def _distinct_streets_count(self, tmp_blocks, ctx):
        return ctx.session.query(tmp_blocks).\
            filter(tmp_blocks.tipo != constants.STREET_TYPE_OTHER).\
            distinct(tmp_blocks.nomencla).\
            count()

    def _streets_query(self, tmp_blocks):
        fields = [
            func.min(tmp_blocks.ogc_fid).label('ogc_fid'),
            tmp_blocks.nomencla,
            func.min(tmp_blocks.nombre).label('nombre'),
            func.min(tmp_blocks.tipo).label('tipo'),
            func.min(tmp_blocks.desdei.cast(Integer)).label('desdei'),
            func.min(tmp_blocks.desded.cast(Integer)).label('desded'),
            func.max(tmp_blocks.hastai.cast(Integer)).label('hastai'),
            func.max(tmp_blocks.hastad.cast(Integer)).label('hastad'),
            tmp_blocks.geom.ST_Union().label('geom')
        ]

        return select(fields).\
            group_by(tmp_blocks.nomencla).\
            where(tmp_blocks.tipo != constants.STREET_TYPE_OTHER)

    def _run_internal(self, tmp_blocks, ctx):
        self._patch_tmp_blocks(tmp_blocks, ctx)
        ctx.session.query(Street).delete()

        bulk_size = ctx.config.getint('etl', 'bulk_size')
        cached_session = ctx.cached_session()
        entities = []
        errors = []
        query = self._streets_query(tmp_blocks)

        ctx.report.info('Calculando cantidad de calles...')
        count = self._distinct_streets_count(tmp_blocks, ctx)
        ctx.report.info('Calles: {}'.format(count))

        ctx.report.info('Generando calles a partir de cuadras...')

        for tmp_block in utils.pbar(ctx.engine.execute(query), ctx,
                                    total=count):
            try:
                street = self._street_from_block(tmp_block, cached_session)
                entities.append(street)

                if len(entities) > bulk_size:
                    ctx.session.add_all(entities)
                    entities.clear()
            except ValidationException:
                errors.append(tmp_block.nomencla)

        ctx.session.add_all(entities)

        ctx.report.info('Errores: {}'.format(len(errors)))
        report_data = ctx.report.get_data(self.name)
        report_data['errors'] = errors
        report_data['streets_count'] = count

        return Street

    def _street_from_block(self, block, cached_session):
        street_id = block.nomencla
        prov_id = street_id[:constants.PROVINCE_ID_LEN]
        dept_id = street_id[:constants.DEPARTMENT_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        department = cached_session.query(Department).get(dept_id)
        if not department:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        return Street(
            id=street_id,
            nombre=utils.clean_string(block.nombre),
            categoria=utils.clean_string(block.tipo),
            fuente=constants.STREETS_SOURCE,
            inicio_derecha=block.desded or 0,
            fin_derecha=block.hastad or 0,
            inicio_izquierda=block.desdei or 0,
            fin_izquierda=block.hastai or 0,
            geometria=block.geom,
            provincia_id=province.id,
            departamento_id=department.id
        )
