from .exceptions import ValidationException
from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, CompositeStep
from .models import Province, Department, LocalGovernment, CensusLocality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    # Utilizar kebab-case en lugar de snake_case para nombres de archivos
    file_basename = constants.CENSUS_LOCALITIES.replace('_', '-')

    return Process(constants.CENSUS_LOCALITIES, [
        utils.CheckDependenciesStep([Province, Department, LocalGovernment]),
        extractors.DownloadURLStep(constants.CENSUS_LOCALITIES + '.zip',
                                   config.get('etl', 'census_localities_url'), constants.CENSUS_LOCALITIES),
        transformers.ExtractZipStep(''),
        loaders.Ogr2ogrStep(table_name=constants.CENSUS_LOCALITIES_TMP_TABLE,
                            geom_type='Point',
                            env={'SHAPE_ENCODING': 'ISO-8859-1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fid': 'numeric',
            'provincia': 'varchar',
            'departamen': 'varchar',
            'cpr': 'varchar',
            'cde': 'varchar',
            'fna': 'varchar',
            'clc': 'varchar',
            'tlc': 'varchar',
            'nomenv': 'varchar',
            'ceu': 'varchar',
            'nomgl': 'varchar',
            'codgl': 'varchar',
            'sag': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            CensusLocalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'census_localities_target_size'),
            op='ge'),
        CompositeStepCreateFile(CensusLocality, 'census_localities', config),
        CompositeStepCopyFile('census_localities', config),
    ])


class CensusLocalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('census_localities_extraction', CensusLocality,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='clc')

    def _patch_tmp_entities(self, tmp_census_localities, ctx):

        # TODO: Averiguar por qué aparecen distintas localidad con el mismo 'clc'
        patch.delete(tmp_census_localities, ctx, clc='06007110')
        patch.delete(tmp_census_localities, ctx, clc='38007010')

        # Se toma como válida la localidad censal con fid=3059
        patch.delete(tmp_census_localities, ctx, clc='70077010', fid='3058')

        # Se toma como válida la localidad censal con fid=3067
        patch.delete(tmp_census_localities, ctx, clc='70098010', fid='3066')

    def _process_entity(self, tmp_census_locality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_census_locality.geom,
                                                     ctx)
        loc_id = tmp_census_locality.clc
        prov_id = loc_id[:constants.PROVINCE_ID_LEN]
        dept_id = loc_id[:constants.DEPARTMENT_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        # El departamento '02000' tiene un significado especial; ver comentario
        # en constants.py.
        department = cached_session.query(Department).get(dept_id)
        if not department and dept_id != constants.CABA_VIRTUAL_DEPARTMENT_ID:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        local_government = geometry.get_entity_at_point(LocalGovernment,
                                                    tmp_census_locality.geom,
                                                    ctx)

        category = constants.CENSUS_LOCALITY_TYPES[tmp_census_locality.tlc]
        # TODO: Pendiente de incorporación
        # function = constants.CENSUS_LOCALITY_ADMIN_FUNCTIONS[tmp_census_locality.func_loc]
        function = None

        return CensusLocality(
            id=loc_id,
            nombre=utils.clean_string(tmp_census_locality.fna),
            categoria=category,
            funcion=function,
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            gobierno_local_id=local_government.id if local_government else None,
            fuente=constants.CENSUS_LOCALITIES_SOURCE,
            geometria=tmp_census_locality.geom
        )
