from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Department, Municipality, CensusLocality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    # Utilizar kebab-case en lugar de snake_case para nombres de archivos
    file_basename = constants.CENSUS_LOCALITIES.replace('_', '-')

    return Process(constants.CENSUS_LOCALITIES, [
        utils.CheckDependenciesStep([Province, Department, Municipality]),
        extractors.DownloadURLStep(constants.CENSUS_LOCALITIES + '.zip',
                                   config.get('etl', 'census_localities_url')),
        transformers.ExtractZipStep(''),
        loaders.Ogr2ogrStep(table_name=constants.CENSUS_LOCALITIES_TMP_TABLE,
                            geom_type='Point',
                            env={'SHAPE_ENCODING': 'utf-8'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'clc': 'varchar',
            'cpr': 'varchar',
            'cde': 'varchar',
            # 'codloc': 'varchar',
            'nompcia': 'varchar',
            'nomdpto': 'varchar',
            'fna': 'varchar',
            # 'func_loc': 'varchar',
            'tipo_asent': 'varchar',
            # 'tip2loc': 'varchar',
            'lat_gs': 'varchar',
            'long_gs': 'varchar',
            'lat_gd': 'numeric',
            'long_gd': 'numeric',
            # 'varones': 'numeric',
            # 'mujeres': 'numeric',
            # 'personas': 'numeric',
            # 'hogares': 'numeric',
            # 'viv_part_h': 'numeric',
            # 'viv_part': 'numeric',
            'geom': 'geometry',

            'sag': 'varchar',
            'nomenv': 'varchar',
            'ceu': 'varchar',
            'nomgl': 'varchar',
            'codgl': 'varchar'
        }),
        CompositeStep([
            CensusLocalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'census_localities_target_size'),
            op='ge'),
        CompositeStep([
            loaders.CreateJSONFileStep(CensusLocality, constants.ETL_VERSION,
                                       file_basename + '.json'),
            loaders.CreateGeoJSONFileStep(CensusLocality,
                                          constants.ETL_VERSION,
                                          file_basename + '.geojson'),
            loaders.CreateCSVFileStep(CensusLocality, constants.ETL_VERSION,
                                      file_basename + '.csv'),
            loaders.CreateNDJSONFileStep(CensusLocality, constants.ETL_VERSION,
                                         file_basename + '.ndjson')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path, file_basename + '.json'),
            utils.CopyFileStep(output_path, file_basename + '.geojson'),
            utils.CopyFileStep(output_path, file_basename + '.csv'),
            utils.CopyFileStep(output_path, file_basename + '.ndjson')
        ])
    ])


class CensusLocalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('census_localities_extraction', CensusLocality,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='clc')

    def _patch_tmp_entities(self, tmp_census_localities, ctx):
        def update_ushuaia(row):
            row.link = '94015' + row.link[constants.DEPARTMENT_ID_LEN:]

        # Actualizar localidades censales de Ushuaia (agregado en ETL2)
        patch.apply_fn(tmp_census_localities, update_ushuaia, ctx,
                       tmp_census_localities.clc.like('94014%'))

        def update_rio_grande(row):
            row.link = '94008' + row.link[constants.DEPARTMENT_ID_LEN:]

        # Actualizar localidades censales de Río Grande (agregado en ETL2)
        patch.apply_fn(tmp_census_localities, update_rio_grande, ctx,
                       tmp_census_localities.clc.like('94007%'))

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

        municipality = geometry.get_entity_at_point(Municipality,
                                                    tmp_census_locality.geom,
                                                    ctx)

        category = constants.CENSUS_LOCALITY_TYPES[tmp_census_locality.tipo_asent]
        # function = constants.CENSUS_LOCALITY_ADMIN_FUNCTIONS[
            # tmp_census_locality.func_loc]

        return CensusLocality(
            id=loc_id,
            nombre=utils.clean_string(tmp_census_locality.fna),
            categoria=category,
            funcion=0,
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            municipio_id=municipality.id if municipality else None,
            fuente=constants.CENSUS_LOCALITIES_SOURCE,
            geometria=tmp_census_locality.geom
        )
