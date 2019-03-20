from .exceptions import ValidationException
from .process import Process, CompositeStep
from .models import Province, Department
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    return Process(constants.DEPARTMENTS, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.DEPARTMENTS + '.zip',
                                   config.get('etl', 'departments_url')),
        transformers.ExtractZipStep(),
        loaders.Ogr2ogrStep(table_name=constants.DEPARTMENTS_TMP_TABLE,
                            geom_type='MultiPolygon', encoding='utf-8'),
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
            DepartmentsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(size=529),
        loaders.CreateJSONFileStep(Department, constants.DEPARTMENTS + '.json')
    ])


class DepartmentsExtractionStep(transformers.EntitiesExtractionStep):
    def __init__(self):
        super().__init__('departments_extraction', Department,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')

    def _patch_tmp_entities(self, tmp_departments, ctx):
        # Antártida Argentina duplicada
        patch.delete(tmp_departments, ctx, ogc_fid=530, in1='94028')

        # Error de tipeo
        patch.update_field(tmp_departments, 'in1', '54084', ctx, in1='55084')

        # Chascomús
        patch.update_field(tmp_departments, 'in1', '06217', ctx, in1='06218')

        # Río Grande
        patch.update_field(tmp_departments, 'in1', '94008', ctx, in1='94007')

        # Ushuaia
        patch.update_field(tmp_departments, 'in1', '94015', ctx, in1='94014')

        # Tolhuin
        patch.update_field(tmp_departments, 'in1', '94011', ctx,
                           fna='Departamento Río Grande', nam='Tolhuin')

    def _process_entity(self, tmp_department, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_department.geom,
                                                     ctx)
        dept_id = tmp_department.in1
        prov_id = dept_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        province_isct = geometry.get_intersection_percentage(
            province.geometria, tmp_department.geom, ctx)

        return Department(
            id=dept_id,
            nombre=utils.clean_string(tmp_department.nam),
            nombre_completo=utils.clean_string(tmp_department.fna),
            categoria=utils.clean_string(tmp_department.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=province.id,
            fuente=utils.clean_string(tmp_department.sag),
            geometria=tmp_department.geom
        )
