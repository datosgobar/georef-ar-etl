from .process import Process, Step, CompositeStep
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                   config.get('etl', 'municipalities_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_RAW_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
        CompositeStep([
            MunicipalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        loaders.CreateJSONFileStep(Municipality,
                                   constants.MUNICIPALITIES + '.json')
    ])


class MunicipalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('municipalities_extraction_step', Municipality)

    def _patch_raw_entities(self, raw_municipalities, ctx):
        patch.delete(raw_municipalities, ctx, in1=None)
        patch.delete(raw_municipalities, ctx, gna=None)

        # TODO: Manejar mejor municipios con IDs inválidos
        patch.delete(raw_municipalities, ctx, in1='82210')
        patch.delete(raw_municipalities, ctx, in1='82287')
        patch.delete(raw_municipalities, ctx, in1='82119')

        patch.update_field(raw_municipalities, 'in1', '540287', ctx,
                           in1='550287')
        patch.update_field(raw_municipalities, 'in1', '540343', ctx,
                           in1='550343')
        patch.update_field(raw_municipalities, 'in1', '820277', ctx,
                           in1='800277')
        patch.update_field(raw_municipalities, 'in1', '585070', ctx,
                           in1='545070')
        patch.update_field(raw_municipalities, 'in1', '589999', ctx,
                           in1='549999')
        patch.update_field(raw_municipalities, 'in1', '629999', ctx,
                           in1='829999')

    def _process_entity(self, raw_municipality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(raw_municipality.geom,
                                                     ctx)
        muni_id = raw_municipality.in1
        prov_id = muni_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        province_isct = geometry.get_intersection_percentage(
            province.geometria, raw_municipality.geom, ctx)

        return Municipality(
            id=muni_id,
            nombre=utils.clean_string(raw_municipality.nam),
            nombre_completo=utils.clean_string(raw_municipality.fna),
            categoria=utils.clean_string(raw_municipality.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=province.id,
            fuente=utils.clean_string(raw_municipality.sag),
            geometria=raw_municipality.geom
        )
