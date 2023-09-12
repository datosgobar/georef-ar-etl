from .process import Process, CompositeStep
from .models import Province
from .exceptions import ValidationException
from . import extractors, transformers, loaders, geometry, utils, constants, patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')
    return Process(constants.PROVINCES, [
        extractors.DownloadURLStep(constants.PROVINCES + '.zip',
                                   config.get('etl', 'provinces_url'), constants.PROVINCES),
        transformers.ExtractZipStep(
            internal_path=""
        ),
        loaders.Ogr2ogrStep(table_name=constants.PROVINCES_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'UTF-8'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'entidad': 'numeric',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'fdc': 'varchar',
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
            loaders.CreateGeoJSONFileStep(
                Province,
                constants.ETL_VERSION,
                constants.PROVINCES + '.geojson',
                tolerance=config.getfloat("etl", "geojson_tolerance"),
                caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
            ),
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

    def _patch_tmp_entities(self, tmp_provinces, ctx):
        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de la provincia ID 94, lanza un error "Self-intersection at or near point
        # [-60.6879223769999,-51.6232448599999]". Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(prov):
            sql_str = """
                    select ST_MakeValid(geom)
                    from {}
                    where in1=:in1
                    limit 1
                    """.format(prov.__table__.name)

            # GeoAlchemy2 no disponibiliza la función ST_MakeValid, utilizar
            # SQL manualmente (como excepción).
            prov.geom = ctx.session.scalar(sql_str, {'in1': prov.in1})

        patch.apply_fn(tmp_provinces, make_valid_geom, ctx, in1='94')
        patch.update_field(tmp_provinces, 'nam', 'Entre Ríos', ctx, nam='Entre Ríoss')

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
