from .process import Process
from . import extractors, transformers, loaders, constants, utils


def create_process(config):
    return Process(constants.COUNTRIES, [
        extractors.DownloadURLStep(constants.COUNTRIES + '.zip',
                                   config.get('etl', 'countries_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.COUNTRIES_RAW_TABLE,
                            geom_type='MultiPolygon', encoding='latin1'),
        utils.DropTableStep()
    ])
