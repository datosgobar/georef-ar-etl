from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Department, Municipality, Locality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.LOCALITIES, [
        utils.CheckDependenciesStep([Province, Department, Municipality]),
        extractors.DownloadURLStep(constants.LOCALITIES + '.tar.gz',
                                   config.get('etl', 'localities_url')),
        transformers.ExtractTarStep(),
        loaders.Ogr2ogrStep(table_name=constants.LOCALITIES_TMP_TABLE,
                            geom_type='MultiPoint', precision=False,
                            env={'SHAPE_ENCODING': 'latin1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'cod_prov': 'varchar',
            'nom_prov': 'varchar',
            'cod_depto': 'varchar',
            'nom_depto': 'varchar',
            'cod_loc': 'varchar',
            'cod_sit': 'varchar',
            'cod_entida': 'varchar',
            'cod_bahra': 'varchar',
            'tipo_bahra': 'varchar',
            'nombre_bah': 'varchar',
            'lat_gd': 'double',
            'long_gd': 'double',
            'lat_gms': 'varchar',
            'long_gms': 'varchar',
            'fuente_ubi': 'varchar',
            'gid': 'integer',
            'geom': 'geometry'
        }),
        CompositeStep([
            LocalitiesExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(size=4101, tolerance=100),
        CompositeStep([
            loaders.CreateJSONFileStep(Locality, constants.ETL_VERSION,
                                       constants.LOCALITIES + '.json'),
            loaders.CreateGeoJSONFileStep(Locality, constants.ETL_VERSION,
                                          constants.LOCALITIES + '.geojson'),
            loaders.CreateCSVFileStep(Locality, constants.ETL_VERSION,
                                      constants.LOCALITIES + '.csv')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path, constants.LOCALITIES + '.json'),
            utils.CopyFileStep(output_path, constants.LOCALITIES + '.geojson'),
            utils.CopyFileStep(output_path, constants.LOCALITIES + '.csv')
        ])
    ])


class LocalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('localities_extraction', Locality,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cod_bahra')

    def _patch_tmp_entities(self, tmp_localities, ctx):
        # Borrar entidades sin ID
        patch.delete(tmp_localities, ctx, cod_bahra=None)

        # Borrar 'EL FICAL'
        patch.delete(tmp_localities, ctx, cod_bahra='70056060001',
                     nombre_bah='EL FICAL')

        # Actualiza códigos para los asentamientos del departamento de Río
        # Grande
        def update_rio_grande(row):
            row.cod_depto = '008'
            row.cod_bahra = (row.cod_prov + row.cod_depto + row.cod_loc +
                             row.cod_entida)
        patch.apply_fn(tmp_localities, update_rio_grande, ctx, cod_prov='94',
                       cod_depto='007')

        # Actualiza códigos para los asentamientos del departamento de Usuhaia
        def update_ushuaia(row):
            row.cod_depto = '015'
            row.cod_bahra = (row.cod_prov + row.cod_depto + row.cod_loc +
                             row.cod_entida)
        patch.apply_fn(tmp_localities, update_ushuaia, ctx, cod_prov='94',
                       cod_depto='014')

    def _build_entities_query(self, tmp_entities, ctx):
        bulk_size = ctx.config.getint('etl', 'bulk_size')
        return ctx.session.query(tmp_entities).\
            filter(tmp_entities.tipo_bahra.in_(constants.BAHRA_TYPES.keys())).\
            yield_per(bulk_size)

    def _process_entity(self, tmp_locality, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_locality.geom,
                                                     ctx)
        loc_id = tmp_locality.cod_bahra
        prov_id = loc_id[:constants.PROVINCE_ID_LEN]
        dept_id = loc_id[:constants.DEPARTMENT_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        # El departamento '02000' tiene un significado especial; ver comentario
        # en constants.py.
        department = cached_session.query(Department).get(dept_id)
        if not department and dept_id != constants.CABA_VIRTUAL_DEPARTMENT_ID:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        municipality = geometry.get_entity_at_point(Municipality,
                                                    tmp_locality.geom, ctx)

        return Locality(
            id=loc_id,
            nombre=utils.clean_string(tmp_locality.nombre_bah),
            categoria=utils.clean_string(tmp_locality.tipo_bahra),
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            municipio_id=municipality.id if municipality else None,
            fuente=utils.clean_string(tmp_locality.fuente_ubi),
            geometria=tmp_locality.geom
        )
