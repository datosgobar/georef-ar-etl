from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Department, Municipality, Locality
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    return Process(constants.LOCALITIES, [
        utils.CheckDependenciesStep([Province, Department, Municipality]),
        extractors.DownloadURLStep(constants.LOCALITIES + '.tar.gz',
                                   config.get('etl', 'localities_url')),
        transformers.ExtractTarStep(),
        loaders.Ogr2ogrStep(table_name=constants.LOCALITIES_TMP_TABLE,
                            geom_type='MultiPoint', encoding='latin1',
                            precision=False),
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
        utils.ValidateTableSizeStep(size=4100, tolerance=100),
        loaders.CreateJSONFileStep(Locality, constants.LOCALITIES + '.json')
    ])


class LocalitiesExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('localities_extraction', Locality,
                         entity_class_pkey='id',
                         tmp_entity_class_pkey='cod_bahra')

    def _patch_tmp_entities(self, tmp_localities, ctx):
        # Agregado en ETL2
        patch.delete(tmp_localities, ctx, cod_bahra='02000010000')

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
        return ctx.session.query(tmp_entities).filter(
            tmp_entities.tipo_bahra.in_(constants.BAHRA_TYPES.keys()))

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

        department = cached_session.query(Department).get(dept_id)
        if not department:
            raise ValidationException(
                'No existe el departamento con ID {}'.format(dept_id))

        municipality = geometry.get_entity_at_point(Municipality,
                                                    tmp_locality.geom, ctx)

        return Locality(
            id=loc_id,
            nombre=utils.clean_string(tmp_locality.nombre_bah),
            categoria=utils.clean_string(tmp_locality.tipo_bahra),
            lon=lon, lat=lat,
            provincia_id=province.id,
            departamento_id=department.id,
            municipio_id=municipality.id if municipality else None,
            fuente=utils.clean_string(tmp_locality.fuente_ubi),
            geometria=tmp_locality.geom
        )
