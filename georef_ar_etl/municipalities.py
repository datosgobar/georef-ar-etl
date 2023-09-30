from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                   config.get('etl', 'municipalities_url'), constants.MUNICIPALITIES),
        transformers.ExtractZipStep(
            internal_path=""
        ),
        loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'utf-8'}),
        utils.ValidateTableSchemaStep({
            'gid': 'numeric',
            'ogc_fid': 'integer',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'fdc': 'varchar',
            'in1': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            MunicipalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'municipalities_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            Municipality, 'municipalities', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('municipalities', config),
    ])


class MunicipalitiesExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('municipalities_extraction', Municipality,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')

    def _patch_tmp_entities(self, tmp_municipalities, ctx):

        patch.delete(tmp_municipalities, ctx, in1=None)
        patch.delete(tmp_municipalities, ctx, gna=None)

        # Se toma como válido el municipio con gid=2086
        patch.delete(tmp_municipalities, ctx, in1='300105', gid='1709')

        # Se toma como válido el municipio con gid=2016
        patch.delete(tmp_municipalities, ctx, in1='302999', gid='1815')

    def _process_entity(self, tmp_municipality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_municipality.geom,
                                                     ctx)
        muni_id = tmp_municipality.in1
        prov_id = muni_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        if ctx.mode == 'normal':
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_municipality.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        return Municipality(
            id=muni_id,
            nombre=utils.clean_string(tmp_municipality.nam),
            nombre_completo=utils.clean_string(tmp_municipality.fna),
            categoria=utils.clean_string(tmp_municipality.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=prov_id,
            fuente=utils.clean_string(tmp_municipality.sag),
            geometria=tmp_municipality.geom
        )
