from .exceptions import ValidationException, ProcessException
from .process import Process, CompositeStep
from .models import Province, Department, Municipality, CensusLocality,\
    Settlement
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.SETTLEMENTS, [
        utils.CheckDependenciesStep([Province, Department, Municipality,
                                     CensusLocality]),
        extractors.DownloadURLStep(constants.SETTLEMENTS + '.tar.gz',
                                   config.get('etl', 'settlements_url')),
        transformers.ExtractTarStep(),
        loaders.Ogr2ogrStep(table_name=constants.SETTLEMENTS_TMP_TABLE,
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
        SettlementsExtractionStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'settlements_target_size'),
            op='ge'),
        CompositeStep([
            loaders.CreateJSONFileStep(Settlement, constants.ETL_VERSION,
                                       constants.SETTLEMENTS + '.json'),
            loaders.CreateGeoJSONFileStep(Settlement, constants.ETL_VERSION,
                                          constants.SETTLEMENTS + '.geojson'),
            loaders.CreateCSVFileStep(Settlement, constants.ETL_VERSION,
                                      constants.SETTLEMENTS + '.csv'),
            loaders.CreateNDJSONFileStep(Settlement, constants.ETL_VERSION,
                                         constants.SETTLEMENTS + '.ndjson')
        ]),
        CompositeStep([
            utils.CopyFileStep(output_path,
                               constants.SETTLEMENTS + '.json'),
            utils.CopyFileStep(output_path,
                               constants.SETTLEMENTS + '.geojson'),
            utils.CopyFileStep(output_path,
                               constants.SETTLEMENTS + '.csv'),
            utils.CopyFileStep(output_path,
                               constants.SETTLEMENTS + '.ndjson')
        ])
    ])


def update_commune_id(row):
    # Multiplicar por 7 los últimos tres dígitos del ID del departamento
    # Ver comentario en constants.py para más detalles.
    prov_id_part = row.cod_bahra[:constants.PROVINCE_ID_LEN]
    dept_id_part = row.cod_bahra[
        constants.PROVINCE_ID_LEN:constants.DEPARTMENT_ID_LEN]
    id_rest = row.cod_bahra[constants.DEPARTMENT_ID_LEN:]

    dept_id_int = int(dept_id_part)
    if dept_id_int > 15:
        # Alguno de los IDs no cumple con la numeración antigua
        raise ProcessException('El ID de comuna {} no es válido.'.format(
            dept_id_part))

    dept_new_id_int = dept_id_int * constants.CABA_MULT_FACTOR
    row.cod_bahra = prov_id_part + str(dept_new_id_int).rjust(
        len(dept_id_part), '0') + id_rest


class SettlementsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self, name='settlements_extraction', entity_class=Settlement):
        super().__init__(name, entity_class, entity_class_pkey='id',
                         tmp_entity_class_pkey='cod_bahra')

    def _patch_tmp_entities(self, tmp_settlements, ctx):
        # Actualizar códigos de comunas (departamentos de CABA)
        patch.apply_fn(tmp_settlements, update_commune_id, ctx,
                       tmp_settlements.cod_bahra.like('02%'))

        # Borrar 'EL FICAL'
        patch.delete(tmp_settlements, ctx, cod_bahra='70056060001',
                     nombre_bah='EL FICAL')

        # Asignarle una localidad censal a "La Toma (Jujuy)"
        patch.update_field(tmp_settlements, 'cod_bahra', '38056025001', ctx,
                           cod_bahra='38056013000')

        # Asignarle una localidad censal a "BARRIO RUTA 24 KILOMETRO 10 (Buenos
        # Aires provincia)"
        patch.update_field(tmp_settlements, 'cod_bahra', '06364030005', ctx,
                           cod_bahra='06364010000')

        # Actualiza códigos para los asentamientos del departamento de Río
        # Grande
        def update_rio_grande(row):
            row.cod_bahra = '94008' + row.cod_bahra[
                constants.DEPARTMENT_ID_LEN:]
        patch.apply_fn(tmp_settlements, update_rio_grande, ctx, cod_prov='94',
                       cod_depto='007')

        # Actualiza códigos para los asentamientos del departamento de Usuhaia
        def update_ushuaia(row):
            row.cod_bahra = '94015' + row.cod_bahra[
                constants.DEPARTMENT_ID_LEN:]
        patch.apply_fn(tmp_settlements, update_ushuaia, ctx, cod_prov='94',
                       cod_depto='014')

    def _process_entity(self, tmp_settlement, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_settlement.geom,
                                                     ctx)
        settlement_id = tmp_settlement.cod_bahra
        prov_id = settlement_id[:constants.PROVINCE_ID_LEN]
        dept_id = settlement_id[:constants.DEPARTMENT_ID_LEN]
        census_loc_id = settlement_id[:constants.CENSUS_LOCALITY_ID_LEN]

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
                                                    tmp_settlement.geom, ctx)

        if prov_id == constants.CABA_PROV_ID:
            # Las calles de CABA pertenecen a la localidad censal 02000010,
            # pero sus IDs *no* comienzan con ese código.
            census_loc_id = constants.CABA_CENSUS_LOCALITY

        census_loc = cached_session.query(CensusLocality).get(
            census_loc_id)

        return Settlement(
            id=settlement_id,
            nombre=utils.clean_string(tmp_settlement.nombre_bah),
            categoria=utils.clean_string(tmp_settlement.tipo_bahra),
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            municipio_id=municipality.id if municipality else None,
            localidad_censal_id=census_loc.id if census_loc else None,
            fuente=utils.clean_string(tmp_settlement.fuente_ubi),
            geometria=tmp_settlement.geom
        )
