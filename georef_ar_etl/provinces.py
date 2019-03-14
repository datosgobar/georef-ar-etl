from .process import Process, CompositeStep
from .models import Province
from . import extractors, transformers, loaders, geometry, utils, constants


def create_process(config):
    return Process(constants.PROVINCES, [
        extractors.DownloadURLStep(constants.PROVINCES + '.zip',
                                   config.get('etl', 'provinces_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.PROVINCES_RAW_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
        CompositeStep([
            ProvincesExtractionStep(),
            utils.DropTableStep()
        ]),
        loaders.CreateJSONFileStep(Province, constants.PROVINCES + '.json')
    ])


class ProvincesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('provinces_extraction', Province,
                         entity_class_pkey='id', raw_entity_class_pkey='in1')
        iso_csv = utils.load_data_csv('iso-3166-provincias-arg.csv')
        self._iso_data = {row['id']: row for row in iso_csv}

    def _process_entity(self, raw_province, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(raw_province.geom, ctx)
        prov_id = raw_province.in1

        return Province(
            id=prov_id,
            nombre=utils.clean_string(raw_province.nam),
            nombre_completo=utils.clean_string(raw_province.fna),
            iso_id=self._iso_data[prov_id]['3166-2 code'],
            iso_nombre=self._iso_data[prov_id]['subdivision name'],
            categoria=utils.clean_string(raw_province.gna),
            lon=lon, lat=lat,
            fuente=utils.clean_string(raw_province.sag),
            geometria=raw_province.geom
        )
