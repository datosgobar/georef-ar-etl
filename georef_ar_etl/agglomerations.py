from .loaders import CompositeStepCopyFile, CompositeStepCreateFile
from .process import Process, CompositeStep
from .models import Agglomerations
from . import extractors, transformers, loaders, geometry, utils, constants, patch


def create_process(config):

    return Process(constants.AGGLOMERATIONS, [
        extractors.DownloadURLStep(constants.AGGLOMERATIONS + '.zip',
                                   config.get('etl', 'agglomerations_url'), constants.AGGLOMERATIONS),
        transformers.ExtractZipStep(''),
        loaders.Ogr2ogrStep(table_name=constants.AGGLOMERATIONS_TMP_TABLE,
                            geom_type='Multipolygon',
                            env={'SHAPE_ENCODING': 'ISO-8859-1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'fid': 'numeric',
            'id': 'varchar',
            'codaglo': 'varchar',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            AgglomerationsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'agglomerations_target_size'),
            op='ge'),
        CompositeStepCreateFile(Agglomerations, 'agglomerations', config),
        CompositeStepCopyFile('agglomerations', config),
    ])


class AgglomerationsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('agglomerations_extraction', Agglomerations,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='codaglo')

    def _patch_tmp_entities(self, tmp_entities, ctx):
        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de algunos gobiernos locales, lanza un error "Self-intersection at or near point..."
        # Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(agglo):
            sql_str = f"""
                SELECT 
                    ST_MakeValid(
                        ST_RemoveRepeatedPoints(geom)
                    )
                
                FROM {agglo.__table__.name}
                WHERE codaglo = :codaglo
                LIMIT 1
            """

            result = ctx.session.execute(sql_str, {'codaglo': agglo.codaglo}).scalar()

            agglo.geom = result

        patch.apply_fn(tmp_entities, make_valid_geom, ctx, codaglo='1495')

    def _process_entity(self, tmp_entity, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_entity.geom,
                                                     ctx)
        agglo_id = tmp_entity.codaglo

        return Agglomerations(
            id=agglo_id,
            nombre=utils.clean_string(tmp_entity.nam),
            fuente=utils.clean_string(tmp_entity.sag),
            categoria=utils.clean_string(tmp_entity.gna),
            nombre_completo=utils.clean_string(tmp_entity.fna),
            lon=lon, lat=lat,
            geometria=tmp_entity.geom
        )
