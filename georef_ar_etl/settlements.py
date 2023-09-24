import os
import subprocess

from .exceptions import ValidationException, ProcessException
from .loaders import OGR2OGR_CMD
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
        extractors.DownloadURLStep(constants.SETTLEMENTS + '.zip',
                                   config.get('etl', 'settlements_url'), constants.SETTLEMENTS),
        ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.SETTLEMENTS_TMP_TABLE,
                            geom_type='MultiPoint', precision=False,
                            env={'SHAPE_ENCODING': 'latin1'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'nombre_geo': 'varchar',
            'tipo_asent': 'varchar',
            'codigo_ase': 'varchar',
            'nombre_agl': 'varchar',
            'codigo_agl': 'varchar',
            'nombre_dep': 'varchar',
            'codigo_ind': 'varchar',
            'nombre_pro': 'varchar',
            'codigo_in0': 'varchar',
            'latitud_gr': 'varchar',
            'longitud_g': 'varchar',
            'latitud_g0': 'varchar',
            'longitud_0': 'varchar',
            'fuente_de_': 'varchar',
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
    prov_id_part = row.codigo_ase[:constants.PROVINCE_ID_LEN]
    dept_id_part = row.codigo_ase[
        constants.PROVINCE_ID_LEN:constants.DEPARTMENT_ID_LEN]
    id_rest = row.codigo_ase[constants.DEPARTMENT_ID_LEN:]

    dept_id_int = int(dept_id_part)
    if dept_id_int > 15:
        # Alguno de los IDs no cumple con la numeración antigua
        raise ProcessException('El ID de comuna {} no es válido.'.format(
            dept_id_part))

    dept_new_id_int = dept_id_int * constants.CABA_MULT_FACTOR
    row.codigo_ase = prov_id_part + str(dept_new_id_int).rjust(
        len(dept_id_part), '0') + id_rest


class ExtractZipStep(transformers.ExtractZipStep):

    def _run_internal(self, filename, ctx):
        file = super()._run_internal(filename, ctx)
        merged_file = self._merge_shp_files(file, ctx)
        return merged_file

    def _merge_shp_files(self, filename_src, ctx):

        path_src = ctx.fs.getsyspath(filename_src)
        shapefile_list = [os.path.join(path_src, f) for f in os.listdir(path_src) if f.endswith('.shp')]

        dirname_merge = os.path.join(filename_src, 'merge/')
        ctx.fs.makedir(dirname_merge)

        file_merge = os.path.join(ctx.fs.getsyspath(dirname_merge), 'merge.shp')

        def run_args(args):
            result = subprocess.run(args)
            if result.returncode:
                raise ProcessException(
                    'El comando ogr2ogr retornó codigo {} al intentar hacer un merge.'.format(
                        result.returncode))

        args = [OGR2OGR_CMD, '-f', 'ESRI Shapefile', '{}'.format(file_merge), '{}'.format(shapefile_list[0])]
        run_args(args)

        for f in shapefile_list[1:]:
            args = [
                OGR2OGR_CMD, '-f', 'ESRI Shapefile',
                '-update', '-append',
                '{}'.format(file_merge), '{}'.format(f),
                '-nln', 'merge'
            ]
            run_args(args)

        return dirname_merge


class SettlementsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self, name='settlements_extraction', entity_class=Settlement):
        super().__init__(name, entity_class, entity_class_pkey='id',
                         tmp_entity_class_pkey='codigo_ase')

    def _patch_tmp_entities(self, tmp_settlements, ctx):
        # Actualizar códigos de comunas (departamentos de CABA)
        patch.apply_fn(tmp_settlements, update_commune_id, ctx,
                       tmp_settlements.codigo_ase.like('02%'))

        def update_rio_grande(row):
            department = geometry.get_entity_at_point(Department, row.geom, ctx)
            row.codigo_ase = department.id + row.codigo_ase[
                constants.DEPARTMENT_ID_LEN:]
        patch.apply_fn(tmp_settlements, update_rio_grande, ctx, codigo_ind='94007')
        patch.apply_fn(tmp_settlements, update_rio_grande, ctx, codigo_ind='94014')

    def _process_entity(self, tmp_settlement, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_settlement.geom,
                                                     ctx)
        settlement_id = tmp_settlement.codigo_ase
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
            nombre=utils.clean_string(tmp_settlement.nombre_geo),
            categoria=utils.clean_string(tmp_settlement.tipo_asent),
            lon=lon, lat=lat,
            provincia_id=prov_id,
            departamento_id=dept_id,
            municipio_id=municipality.id if municipality else None,
            localidad_censal_id=census_loc.id if census_loc else None,
            fuente=utils.clean_string(tmp_settlement.fuente_de_),
            geometria=tmp_settlement.geom
        )
