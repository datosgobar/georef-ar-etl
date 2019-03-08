from .process import Process, Step, MultiStep
from .models import Province
from . import extractors, transformers, loaders, geometry, utils, constants


def create_process(ctx):
    return Process(constants.PROVINCES, [
        extractors.DownloadURLStep(constants.PROVINCES + '.zip',
                                   ctx.config.get('etl', 'provinces_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.PROVINCES_RAW_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
        MultiStep([
            ProvincesExtractionStep(),
            utils.DropTableStep()
        ]),
        loaders.CreateJSONFileStep(Province, constants.PROVINCES + '.json')
    ])


class ProvincesExtractionStep(Step):
    def __init__(self):
        super().__init__('provinces_extraction')

    def _run_internal(self, raw_provinces, ctx):
        provinces = []
        iso_csv = utils.load_data_csv('iso-3166-provincias-arg.csv')
        iso_data = {row['id']: row for row in iso_csv}

        # TODO: Manejar comparación con provincias que ya están en la base
        ctx.query(Province).delete()
        query = ctx.query(raw_provinces)
        count = query.count()

        ctx.logger.info('Insertando provincias procesadas...')

        for raw_province in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid_coordinates(raw_province.geom,
                                                         ctx)
            prov_id = raw_province.in1

            province = Province(
                id=prov_id,
                nombre=utils.clean_string(raw_province.nam),
                nombre_completo=utils.clean_string(raw_province.fna),
                iso_id=iso_data[prov_id]['3166-2 code'],
                iso_nombre=iso_data[prov_id]['subdivision name'],
                categoria=utils.clean_string(raw_province.gna),
                lon=lon, lat=lat,
                fuente=utils.clean_string(raw_province.sag),
                geometria=raw_province.geom
            )

            provinces.append(province)

        ctx.session.add_all(provinces)
