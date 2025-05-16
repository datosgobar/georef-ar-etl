from .exceptions import ValidationException
from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, CompositeStep
from .models import Province, CensusLocality, Department, CensusTracts
from . import extractors, transformers, loaders, geometry, utils, constants

def create_process(config):

    return Process(constants.CENSUS_TRACTS, [
        utils.CheckDependenciesStep([Province, Department]),
        extractors.DownloadURLStep(constants.CENSUS_TRACTS + '.zip',
                                   config.get('etl', 'census_tracts_url'), constants.CENSUS_TRACTS),
        transformers.ExtractZipStep(''),
        loaders.Ogr2ogrStep(table_name=constants.CENSUS_TRACTS_TMP_TABLE,
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
            'cod_indec': 'varchar',
            'sag': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            CensusTractsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'census_tracts_target_size'),
            op='ge'),
        CompositeStepCreateFile(CensusTracts, 'census_tracts', config),
        CompositeStepCopyFile('census_tracts', config),
    ])


class CensusTractsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('census_tracts_extraction', CensusTracts,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cod_indec')

    def _process_entity(self, tmp_entity, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_entity.geom,
                                                     ctx)
        census_tract_id = tmp_entity.cod_indec

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

        if ctx.mode == 'normal':
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_entity.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        return CensusTracts(
            id=census_tract_id,
            fuente=utils.clean_string(tmp_entity.sag),
            provincia_id=prov_id,
            departamento_id=dept_id,
            provincia_interseccion=province_isct,
            lon=lon, lat=lat,
            geometria=tmp_entity.geom
        )
