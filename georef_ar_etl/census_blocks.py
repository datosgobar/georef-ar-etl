from unicodedata import category

from .exceptions import ValidationException
from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, CompositeStep
from .models import Province, CensusBlocks, Department, CensusTracts
from . import extractors, transformers, loaders, geometry, utils, constants, patch


def create_process(config):

    return Process(constants.CENSUS_BLOCKS, [
        utils.CheckDependenciesStep([Province, Department, CensusTracts]),
        extractors.DownloadURLStep(constants.CENSUS_BLOCKS + '.zip',
                                   config.get('etl', 'census_blocks_url'), constants.CENSUS_BLOCKS),
        transformers.ExtractZipStep(''),
        loaders.Ogr2ogrStep(table_name=constants.CENSUS_BLOCKS_TMP_TABLE,
                            geom_type='Multipolygon',
                            env={'SHAPE_ENCODING': 'ISO-8859-1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fid': 'numeric',
            'id': 'varchar',
            'cpr': 'varchar',
            'jur': 'varchar',
            'cde': 'varchar',
            'dpto': 'varchar',
            'cfn': 'varchar',
            'cro': 'varchar',
            'tro': 'varchar',
            'cod_indec': 'varchar',
            'sag': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            CensusBlocksExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'census_blocks_target_size'),
            op='ge'),
        CompositeStepCreateFile(CensusBlocks, 'census_blocks', config),
        CompositeStepCopyFile('census_blocks', config),
    ])


class CensusBlocksExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('census_blocks_extraction', CensusBlocks,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cod_indec')

    def _patch_tmp_entities(self, tmp_entities, ctx):
        patch.update_field(tmp_entities, 'tro', constants.CensusBlockINDECType.N.name, ctx, tro="")
        patch.update_field(tmp_entities, 'tro', constants.CensusBlockINDECType.N.name, ctx, tro=None)

        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de algunos gobiernos locales, lanza un error "Self-intersection at or near point..."
        # Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(cb):
            sql_str = f"""
                SELECT ST_Multi(
                           ST_CollectionExtract(
                               ST_MakeValid(
                                   ST_RemoveRepeatedPoints(geom)
                               ),
                               3
                           )
                       )
                FROM {cb.__table__.name}
                WHERE cod_indec = :cod_indec
                LIMIT 1
            """

            result = ctx.session.execute(sql_str, {'cod_indec': cb.cod_indec}).scalar()

            cb.geom = result

        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_indec='221402104')
        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_indec='221402811')
        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_indec='221470304')
        patch.apply_fn(tmp_entities, make_valid_geom, ctx, cod_indec='221402201')


    def _process_entity(self, tmp_entity, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_entity.geom,
                                                     ctx)
        cb_id = tmp_entity.cod_indec

        prov_id = tmp_entity.cpr
        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        dept_id = tmp_entity.cde
        department = cached_session.query(Department).get(dept_id)
        if not department and dept_id != constants.CABA_VIRTUAL_DEPARTMENT_ID:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        ct_id = cb_id[:-2]
        census_tract = cached_session.query(CensusTracts).get(ct_id)
        if not census_tract:
            raise ValidationException(
                'No existe la fracción censal con ID {}'.format(dept_id))

        if ctx.mode == 'normal':
            census_tract_isct = geometry.get_intersection_percentage(
                census_tract.geometria, tmp_entity.geom, ctx)
        else:
            census_tract_isct = 0  # Saltear operaciones costosas en testing

        category = constants.CENSUS_BLOCK_INDEC_TYPES.get(tmp_entity.tro)

        return CensusBlocks(
            id=cb_id,
            fuente=utils.clean_string(tmp_entity.sag),
            provincia_id=prov_id,
            departamento_id=dept_id,
            fraccion_censal_id=ct_id,
            fraccion_censal_interseccion=census_tract_isct,
            lon=lon, lat=lat,
            categoria=category,
            geometria=tmp_entity.geom
        )
