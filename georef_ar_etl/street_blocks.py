from .process import Process, CompositeStep
from .models import Province, Department
from . import extractors, loaders, utils, constants


def create_process(config):
    url_template = config.get('etl', 'street_blocks_url_template')

    download_cstep = CompositeStep([
        extractors.DownloadURLStep(
            '{}_{}.csv'.format(constants.STREET_BLOCKS, province_id),
            url_template.format(province_id),
            params={
                'CQL_FILTER': 'nomencla like \'{}%\''.format(province_id)
            }
        ) for province_id in constants.PROVINCE_IDS
    ])

    ogr2ogr_cstep = CompositeStep([
        loaders.Ogr2ogrStep(table_name=constants.STREET_BLOCKS_TMP_TABLE,
                            geom_type='MultiLineString', overwrite=False)
    ] * len(download_cstep))

    return Process(constants.STREET_BLOCKS, [
        utils.CheckDependenciesStep([Province, Department]),
        download_cstep,
        ogr2ogr_cstep,
        utils.FirstResultStep
    ])
