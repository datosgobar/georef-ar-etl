from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                   config.get('etl', 'municipalities_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_TMP_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'in1': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            MunicipalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(size=1800, tolerance=100),
        loaders.CreateJSONFileStep(Municipality,
                                   constants.MUNICIPALITIES + '.json')
    ])


class MunicipalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('municipalities_extraction_step', Municipality,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')

    def _patch_tmp_entities(self, tmp_municipalities, ctx):
        patch.delete(tmp_municipalities, ctx, in1=None)
        patch.delete(tmp_municipalities, ctx, gna=None)

        # TODO: Manejar mejor municipios con IDs inválidos
        patch.delete(tmp_municipalities, ctx, in1='82210')
        patch.delete(tmp_municipalities, ctx, in1='82287')
        patch.delete(tmp_municipalities, ctx, in1='82119')

        patch.update_field(tmp_municipalities, 'in1', '540287', ctx,
                           in1='550287')
        patch.update_field(tmp_municipalities, 'in1', '540343', ctx,
                           in1='550343')
        patch.update_field(tmp_municipalities, 'in1', '820277', ctx,
                           in1='800277')
        patch.update_field(tmp_municipalities, 'in1', '585070', ctx,
                           in1='545070')
        patch.update_field(tmp_municipalities, 'in1', '589999', ctx,
                           in1='549999')
        patch.update_field(tmp_municipalities, 'in1', '629999', ctx,
                           in1='829999')

    def _process_entity(self, tmp_municipality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_municipality.geom,
                                                     ctx)
        muni_id = tmp_municipality.in1
        prov_id = muni_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        province_isct = geometry.get_intersection_percentage(
            province.geometria, tmp_municipality.geom, ctx)

        return Municipality(
            id=muni_id,
            nombre=utils.clean_string(tmp_municipality.nam),
            nombre_completo=utils.clean_string(tmp_municipality.fna),
            categoria=utils.clean_string(tmp_municipality.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=province.id,
            fuente=utils.clean_string(tmp_municipality.sag),
            geometria=tmp_municipality.geom
        )
