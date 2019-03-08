from .process import Process, Step, MultiStep
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(ctx):
    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                   ctx.config.get('etl',
                                                  'municipalities_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_RAW_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
        MultiStep([
            MunicipalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FunctionStep(lambda results: results[0])
    ])


class MunicipalitiesExtractionStep(Step):
    def __init__(self):
        super().__init__('municipalities_extraction_step')

    def _patch_raw_municipalities(self, raw_municipalities, ctx):
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

    def _run_internal(self, raw_municipalities, ctx):
        self._patch_raw_municipalities(raw_municipalities, ctx)
        municipalities = []

        # TODO: Manejar comparación con municipios que ya están en la base
        ctx.query(Municipality).delete()
        query = ctx.query(raw_municipalities)
        count = query.count()

        ctx.logger.info('Insertando municipios procesados...')

        for raw_municipality in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid_coordinates(raw_municipality.geom,
                                                         ctx)
            muni_id = raw_municipality.in1
            prov_id = muni_id[:constants.PROVINCE_ID_LEN]

            province = ctx.query(Province).get(prov_id)
            province_isct = geometry.get_intersection_percentage(
                province.geometria, raw_municipality.geom, ctx)

            municipality = Municipality(
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

            municipalities.append(municipality)

        ctx.session.add_all(municipalities)
