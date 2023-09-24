from sqlalchemy.sql import select, func
from sqlalchemy.sql.sqltypes import Integer
from .exceptions import ValidationException
from .process import Process, CompositeStep, StepSequence
from .models import Province, Department, CensusLocality, Street
from . import extractors, loaders, utils, constants, patch, transformers

INVALID_BLOCKS_CENSUS_LOCALITIES = [
    # '62042450',
    # '74056100',
    # '74056150',
    # '14098230',
    # '14098170',
    # '58042010',
    # '06778020',
    # '42133050'
]

INVALID_BLOCKS_CLC = {
    # '06028028': '06028010',
    # '06035035': '06035010',
    # '06091091': '06091010',
    # '06098098': '06098010',
    # '06105030': '06105040',
    # '06105040': '06105050',
    # '06105050': '06105060',
    # '06105060': '06105070',
    # '06105070': '06105080',
    # '06245245': '06245010',
    # '06252252': '06252010',
    # '06260260': '06260010',
    # '06266020': '06266100',
    # '06270270': '06270010',
    # '06274274': '06274010',
    # '06364364': '06364030',
    # '06371371': '06371010',
    # '06408408': '06408010',
    # '06410410': '06410010',
    # '06412412': '06412010',
    # '06427427': '06427010',
    # '06434434': '06434010',
    # '06441441': '06441030',
    # '06490490': '06490010',
    # '06515515': '06515010',
    # '06525525': '06525020',
    # '06539539': '06539010',
    # '06560560': '06560010',
    # '06568568': '06568010',
    # '06638638': '06638040',
    # '06648648': '06648010',
    # '06658658': '06658010',
    # '06749749': '06749010',
    # '06756756': '06756010',
    # '06760760': '06760010',
    # '06763070': '06763060',
    # '06778778': '06778020',
    # '06805805': '06805010',
    # '06833040': '06833050',
    # '06833050': '06833060',
    # '06833060': '06833070',
    # '06833070': '06833080',
    # '06833080': '06833090',
    # '06833090': '06833100',
    # '06833100': '06833110',
    # '06840840': '06840010',
    # '06847030': '06847020',
    # '06861861': '06861010',
    # '14175103': '14049112',
    # '30021140': '30021125',
    # '30021140': '30021128',
    # '38007093': '38007092',
    # '66105010': '38084055',
    # '42133020': '42133050',
    # '50049250': '50049015',
    # '50063090': '50063070',
    # '50070090': '50070100',
    # '54007010': '54007025',
    # '38084055': '66105010',
    # '70098010': '70063010',
    # '82105240': '82105250',
    # '82119020': '82119150',
    # '86049010': '86049015',
    # '86049120': '86049040',
    # '86049050': '86049060',
    # '86049050': '86049120',
    # '86147050': '86091040',
    # '86091050': '86091060',
    # '86147060': '86147095',
    # '86147130': '86147123',
    # '90098040': '90098035',
}


def create_process(config):
    url_template = config.get('etl', 'street_blocks_url_template')
    output_path = config.get('etl', 'output_dest_path')

    download_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.geojson'.format(constants.STREET_BLOCKS, province_id),
            url_template.format(province_id),
            constants.STREET_BLOCKS
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

    url_template_street = config.get('etl', 'streets_url_template')
    download_cstep_streets = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.geojson'.format(constants.STREETS, province_id),
            url_template_street.format(province_id),
            constants.STREETS
        ) for province_id in constants.PROVINCE_IDS
    ], name='download_cstep_streets')

    ogr2ogr_cstep_streets = CompositeStep([
      loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
                          geom_type='MultiLineString',
                          source_epsg='EPSG:4326')
    ] + [
      loaders.Ogr2ogrStep(table_name=constants.STREETS_TMP_TABLE,
                          geom_type='MultiLineString', overwrite=False,
                          source_epsg='EPSG:4326')
    ] * (len(download_cstep) - 1), name='ogr2ogr_cstep_streets')

    return Process(constants.STREETS, [
        utils.CheckDependenciesStep([Province, Department, CensusLocality]),
        CompositeStep([
            StepSequence([
                download_cstep,
                ogr2ogr_cstep,
                utils.FirstResultStep,
                utils.ValidateTableSchemaStep({
                    'id': 'integer',
                    'nomencla': 'varchar',
                    'tipo': 'varchar',
                    'nombre': 'varchar',
                    'desdei': 'integer',
                    'desded': 'integer',
                    'hastad': 'integer',
                    'hastai': 'integer',
                    'codloc20': 'varchar',
                    'geom': 'geometry'
                })
            ], name='load_tmp_street_blocks'),
            StepSequence([
                download_cstep_streets,
                ogr2ogr_cstep_streets,
                utils.FirstResultStep,
                utils.ValidateTableSchemaStep({
                    'id': 'integer',
                    'nomencla': 'varchar',
                    'tipo': 'varchar',
                    'nombre': 'varchar',
                    'desdei': 'varchar',
                    'desded': 'varchar',
                    'hastai': 'varchar',
                    'hastad': 'varchar',
                    'codloc': 'varchar',
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
            loaders.CreateGeoJSONFileStep(
                Street,
                constants.ETL_VERSION,
                constants.STREETS + '.geojson',
                tolerance=config.getfloat("etl", "geojson_tolerance"),
                caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
            ),
            loaders.CreateCSVFileStep(Street, constants.ETL_VERSION,
                                      constants.STREETS + '.csv'),
            loaders.CreateNDJSONFileStep(Street, constants.ETL_VERSION,
                                         constants.STREETS + '.ndjson')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path, constants.STREETS + '.json'),
            utils.CopyFileStep(output_path, constants.STREETS + '.geojson'),
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

        patch.delete(tmp_blocks, ctx, tipo='')

        patch.delete(tmp_blocks, ctx, nombre='')

        # Una cuadra de la calle "064414417007012" no contiene geometría
        patch.delete(tmp_blocks, ctx, geom=None)

        patch.delete(tmp_blocks, ctx, nomencla='4213305000025')
        patch.delete(tmp_blocks, ctx, nomencla='4213305000030')
        patch.delete(tmp_blocks, ctx, nomencla='4213305000075')

        def update_clc(row):
            old_clc = row.nomencla[:constants.CENSUS_LOCALITY_ID_LEN]
            new_clc = INVALID_BLOCKS_CLC.get(old_clc)
            row.nomencla = new_clc + row.nomencla[constants.CENSUS_LOCALITY_ID_LEN:]
            row.codloc20 = new_clc

        for clc in INVALID_BLOCKS_CLC.keys():
            patch.apply_fn(tmp_blocks, update_clc, ctx, tmp_blocks.nomencla.like('{}%'.format(clc)))

        ctx.session.commit()

    def _entities_query_count(self, tmp_blocks, ctx):
        return ctx.session.query(tmp_blocks).\
            filter(tmp_blocks.tipo != constants.STREET_TYPE_OTHER).\
            distinct(tmp_blocks.nomencla).\
            count()

    def _build_entities_query(self, tmp_blocks, ctx):
        fields = [
            func.min(tmp_blocks.id).label('id'),
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
