from .process import Process, CompositeStep
from .models import Province
from .exceptions import ValidationException
from . import extractors, transformers, loaders, geometry, utils, constants


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.PROVINCES, [
        extractors.DownloadURLStep(constants.PROVINCES + '.zip',
                                   config.get('etl', 'provinces_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.PROVINCES_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'utf-8'}),
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
            ProvincesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'provinces_target_size')),
        CompositeStep([
            loaders.CreateJSONFileStep(Province, constants.ETL_VERSION,
                                       constants.PROVINCES + '.json'),
            loaders.CreateGeoJSONFileStep(Province, constants.ETL_VERSION,
                                          constants.PROVINCES + '.geojson'),
            loaders.CreateCSVFileStep(Province, constants.ETL_VERSION,
                                      constants.PROVINCES + '.csv'),
            loaders.CreateNDJSONFileStep(Province, constants.ETL_VERSION,
                                         constants.PROVINCES + '.ndjson')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path, constants.PROVINCES + '.json'),
            utils.CopyFileStep(output_path, constants.PROVINCES + '.geojson'),
            utils.CopyFileStep(output_path, constants.PROVINCES + '.csv'),
            utils.CopyFileStep(output_path, constants.PROVINCES + '.ndjson')
        ])
    ])


class ProvincesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('provinces_extraction', Province,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')
        iso_csv = utils.load_data_csv('iso-3166-provincias-arg.csv')
        self._iso_data = {row['id']: row for row in iso_csv}

    def _process_entity(self, tmp_province, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_province.geom, ctx)
        prov_id = tmp_province.in1

        try:
            iso_id = self._iso_data[prov_id]['3166-2 code']
            iso_nombre = self._iso_data[prov_id]['subdivision name']
        except KeyError:
            raise ValidationException('ID de provincia inválido')

        return Province(
            id=prov_id,
            nombre=utils.clean_string(tmp_province.nam),
            nombre_completo=utils.clean_string(tmp_province.fna),
            iso_id=iso_id,
            iso_nombre=iso_nombre,
            categoria=utils.clean_string(tmp_province.gna),
            lon=lon, lat=lat,
            fuente=utils.clean_string(tmp_province.sag),
            geometria=tmp_province.geom
        )
