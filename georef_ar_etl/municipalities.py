from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Municipality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.MUNICIPALITIES, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.MUNICIPALITIES + '.zip',
                                   config.get('etl', 'municipalities_url')),
        transformers.ExtractZipStep(
            internal_path=""
        ),
        loaders.Ogr2ogrStep(table_name=constants.MUNICIPALITIES_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'utf-8'}),
        utils.ValidateTableSchemaStep({
            'gid': 'numeric',
            'ogc_fid': 'integer',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'fdc': 'varchar',
            'in1': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            MunicipalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'municipalities_target_size'),
            op='ge'),
        CompositeStep([
            loaders.CreateJSONFileStep(
                Municipality, constants.ETL_VERSION,
                constants.MUNICIPALITIES + '.json'),
            loaders.CreateGeoJSONFileStep(
                Municipality, constants.ETL_VERSION,
                constants.MUNICIPALITIES + '.geojson',
                tolerance=config.getfloat("etl", "geojson_tolerance"),
                caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
            ),
            loaders.CreateCSVFileStep(
                Municipality, constants.ETL_VERSION,
                constants.MUNICIPALITIES + '.csv'),
            loaders.CreateNDJSONFileStep(
                Municipality, constants.ETL_VERSION,
                constants.MUNICIPALITIES + '.ndjson'
            )
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path,
                               constants.MUNICIPALITIES + '.json'),
            utils.CopyFileStep(output_path,
                               constants.MUNICIPALITIES + '.geojson'),
            utils.CopyFileStep(output_path,
                               constants.MUNICIPALITIES + '.csv'),
            utils.CopyFileStep(output_path,
                               constants.MUNICIPALITIES + '.ndjson')
        ])
    ])


class MunicipalitiesExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('municipalities_extraction', Municipality,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')

    def _patch_tmp_entities(self, tmp_municipalities, ctx):

        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de algunos municipios, lanza un error "Self-intersection at or near point..."
        # Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(mun):
            sql_str = """
                            select ST_MakeValid(geom)
                            from {}
                            where in1=:in1
                            limit 1
                            """.format(mun.__table__.name)

            # GeoAlchemy2 no disponibiliza la función ST_MakeValid, utilizar
            # SQL manualmente (como excepción).
            mun.geom = ctx.session.scalar(sql_str, {'in1': mun.in1})

        patch.apply_fn(tmp_municipalities, make_valid_geom, ctx, in1='060056')
        patch.apply_fn(tmp_municipalities, make_valid_geom, ctx, in1='180455')
        patch.apply_fn(tmp_municipalities, make_valid_geom, ctx, in1='180224')
        patch.apply_fn(tmp_municipalities, make_valid_geom, ctx, in1='180077')

        patch.delete(tmp_municipalities, ctx, in1=None)
        patch.delete(tmp_municipalities, ctx, gna=None)

        # TODO: Manejar mejor municipios con IDs inválidos
        patch.delete(tmp_municipalities, ctx, in1='82210')
        patch.delete(tmp_municipalities, ctx, in1='82287')
        patch.delete(tmp_municipalities, ctx, in1='82119')

        # TODO: Verificar por qué traían más de un registro con el mismo in1
        # Se toma como válido el municipio con gid=1965
        patch.delete(tmp_municipalities, ctx, in1='300182', gid='1966')
        patch.delete(tmp_municipalities, ctx, in1='300182', gid='1787')

        # Se toma como válido el municipio con gid=1838
        patch.delete(tmp_municipalities, ctx, in1='300434', gid='1906')

        # Se toma como válido el municipio con gid=2021
        patch.delete(tmp_municipalities, ctx, in1='300448', gid='1881')

        # Se toma como válido el municipio con gid=1880
        patch.delete(tmp_municipalities, ctx, in1='300301', gid='1825')

        # Se toma como válido el municipio con gid=2037
        patch.delete(tmp_municipalities, ctx, in1='309605', gid='1869')

        patch.update_field(tmp_municipalities, 'in1', '540287', ctx,
                           in1='550287')
        patch.update_field(tmp_municipalities, 'in1', '540343', ctx,
                           in1='550343')
        patch.update_field(tmp_municipalities, 'in1', '820277', ctx,
                           in1='800277')

    def _process_entity(self, tmp_municipality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_municipality.geom,
                                                     ctx)
        muni_id = tmp_municipality.in1
        prov_id = muni_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        if ctx.mode == 'normal':
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_municipality.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        return Municipality(
            id=muni_id,
            nombre=utils.clean_string(tmp_municipality.nam),
            nombre_completo=utils.clean_string(tmp_municipality.fna),
            categoria=utils.clean_string(tmp_municipality.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=prov_id,
            fuente=utils.clean_string(tmp_municipality.sag),
            geometria=tmp_municipality.geom
        )
