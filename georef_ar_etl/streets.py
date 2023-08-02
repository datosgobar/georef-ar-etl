from sqlalchemy.sql import select, func
from sqlalchemy.sql.sqltypes import Integer
from .exceptions import ValidationException
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, CensusLocality, Street
from . import extractors, loaders, utils, constants, patch, transformers

INVALID_BLOCKS_CENSUS_LOCALITIES = [
    '62042450',
    '74056100',
    '74056150',
    '14098230',
    '14098170',
    '58042010',
    '06778020'
]


def create_process(config):
    url_template = config.get('etl', 'street_blocks_url_template')
    output_path = config.get('etl', 'output_dest_path')

    download_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.csv'.format(constants.STREET_BLOCKS, province_id),
            url_template.format(province_id),
            constants.STREET_BLOCKS,
            params={
                'CQL_FILTER': 'nomencla like \'{}%\''.format(province_id)
            }
        ) for province_id in constants.PROVINCE_IDS
    ], name='download_cstep')

    ogr2ogr_cstep = CompositeStep([
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString',
                            source_epsg='EPSG:4326')
    ] + [
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString', overwrite=False,
                            source_epsg='EPSG:4326')
    ] * (len(download_cstep) - 1), name='ogr2ogr_cstep')

    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department, CensusLocality]),
        CompositeStep([
            StepSequence([
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
                })
            ], name='load_tmp_street_blocks'),
            StepSequence([
                extractors.DownloadURLStep(constants.STREETS + '.zip',
                                           config.get('etl', 'streets_url'), constants.STREETS),
                transformers.ExtractZipStep(),
                loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
                                    geom_type='MultiLineString',
                                    env={'SHAPE_ENCODING': 'latin1'}),
                utils.ValidateTableSchemaStep({
                    'ogc_fid': 'integer',
                    'nomencla': 'varchar',
                    'codigo': 'double',
                    'tipo': 'varchar',
                    'nombre': 'varchar',
                    'desdei': 'double',
                    'desded': 'double',
                    'hastai': 'double',
                    'hastad': 'double',
                    'codloc': 'varchar',
                    'codaglo': 'varchar',
                    'link': 'varchar',
                    'geom': 'geometry'
                })
            ], name='load_tmp_streets')
        ]),
        StreetsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'streets_target_size'),
            op='ge'),
        CompositeStep([
            loaders.CreateJSONFileStep(Street, constants.ETL_VERSION,
                                       constants.STREETS + '.json'),
            loaders.CreateCSVFileStep(Street, constants.ETL_VERSION,
                                      constants.STREETS + '.csv'),
            loaders.CreateNDJSONFileStep(Street, constants.ETL_VERSION,
                                         constants.STREETS + '.ndjson')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path, constants.STREETS + '.json'),
            utils.CopyFileStep(output_path, constants.STREETS + '.csv'),
            utils.CopyFileStep(output_path, constants.STREETS + '.ndjson')
        ])
    ])


class StreetsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('streets_extraction', Street,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='nomencla')

    def _patch_tmp_entities(self, tmp_blocks, ctx):
        def update_marcos_paz(row):
            row.nomencla = '06525020' + row.nomencla[
                constants.CENSUS_LOCALITY_ID_LEN:]

        # Asignar localidad censal a las calles de Marcos Paz que no
        # la tienen asignada.
        patch.apply_fn(tmp_blocks, update_marcos_paz, ctx,
                       tmp_blocks.nomencla.like('06525999%'))

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

    def _entities_query_count(self, tmp_blocks, ctx):
        return ctx.session.query(tmp_blocks).\
            filter(tmp_blocks.tipo != constants.STREET_TYPE_OTHER).\
            distinct(tmp_blocks.nomencla).\
            count()

    def _build_entities_query(self, tmp_blocks, ctx):
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

        statement = select(fields).\
            group_by(tmp_blocks.nomencla).\
            where(tmp_blocks.tipo != constants.STREET_TYPE_OTHER)

        return ctx.engine.execute(statement)

    def _copy_tmp_streets(self, tmp_blocks, tmp_streets, ctx):
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        ctx.report.info(
            'Copiando calles de localidades con cuadras inválidas...')

        blocks = []
        for census_locality_id in INVALID_BLOCKS_CENSUS_LOCALITIES:
            ctx.report.info('Localidad Censal ID {}'.format(
                census_locality_id))

            query = ctx.session.query(tmp_streets).\
                filter_by(codloc=census_locality_id).\
                yield_per(bulk_size)

            for tmp_street in query:
                blocks.append(tmp_blocks(
                    geom=tmp_street.geom,
                    codloc20=tmp_street.codloc,
                    nomencla=tmp_street.nomencla,
                    nombre=tmp_street.nombre,
                    tipo=tmp_street.tipo,
                    desded=int(tmp_street.desded),
                    desdei=int(tmp_street.desdei),
                    hastad=int(tmp_street.hastad),
                    hastai=int(tmp_street.hastai)
                ))

        ctx.session.add_all(blocks)
        ctx.session.commit()
        ctx.report.info('Terminado.\n')

    def _run_internal(self, data, ctx):
        tmp_blocks, tmp_streets = data

        if tmp_streets:
            for census_locality_id in INVALID_BLOCKS_CENSUS_LOCALITIES:
                patch.delete(tmp_blocks, ctx, codloc20=census_locality_id)

            self._copy_tmp_streets(tmp_blocks, tmp_streets, ctx)

        return super()._run_internal(tmp_blocks, ctx)

    def _process_entity(self, block, cached_session, ctx):
        street_id = block.nomencla
        prov_id = street_id[:constants.PROVINCE_ID_LEN]
        dept_id = street_id[:constants.DEPARTMENT_ID_LEN]
        census_loc_id = street_id[:constants.CENSUS_LOCALITY_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        department = cached_session.query(Department).get(dept_id)
        if not department:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        if prov_id == constants.CABA_PROV_ID:
            # Las calles de CABA pertenecen a la localidad censal 02000010,
            # pero sus IDs *no* comienzan con ese código.
            census_loc_id = constants.CABA_CENSUS_LOCALITY

        census_locality = cached_session.query(CensusLocality).get(
            census_loc_id)
        if not census_locality:
            raise ValidationException(
                'No existe la localidad censal con ID {}'.format(
                    census_loc_id))

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
            provincia_id=prov_id,
            departamento_id=dept_id,
            localidad_censal_id=census_loc_id
        )
