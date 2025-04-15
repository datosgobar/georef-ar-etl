from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep, StepSequence
from .models import Province, LocalGovernment
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):

    return Process(constants.LOCAL_GOVERNMENTS, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.LOCAL_GOVERNMENTS + '.zip',
                                   config.get('etl', 'local_governments_indec_url'), constants.LOCAL_GOVERNMENTS),
        transformers.ExtractZipStep(
            internal_path=""
        ),
        loaders.Ogr2ogrStep(table_name=constants.LOCAL_GOVERNMENTS_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'ISO-8859-1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'fdc': 'varchar',
            'fid': 'numeric',
            'id': 'varchar',
            'cpr': 'varchar',
            'jur': 'varchar',
            'cmu': 'varchar',
            'cat_gl': 'varchar',
            'legisla': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            LocalGovernmentsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'local_governments_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            LocalGovernment, 'local_governments', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('local_governments', config),
    ])


class LocalGovernmentsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('local_governments_extraction', LocalGovernment,
                         entity_class_pkey='id', tmp_entity_class_pkey='cmu')

    def _patch_tmp_entities(self, tmp_entities, ctx):
        patch.delete(tmp_entities, ctx, nam=None)
        patch.delete(tmp_entities, ctx, fna=None)

    def _process_entity(self, tmp_local_government, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_local_government.geom, ctx)

        lg_id = tmp_local_government.cmu
        prov_id = lg_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        if ctx.mode == 'normal' and tmp_local_government.geom is not None:
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_local_government.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        categoria = tmp_local_government.gna
        nombre = tmp_local_government.nam
        nombre_completo = tmp_local_government.fna

        return LocalGovernment(
            id=lg_id,
            nombre=utils.clean_string(nombre),
            nombre_completo=utils.clean_string(nombre_completo),
            categoria=utils.clean_string(categoria),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=prov_id,
            fuente=tmp_local_government.sag or "",
            geometria=tmp_local_government.geom
        )
