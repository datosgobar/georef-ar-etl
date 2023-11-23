from .exceptions import ValidationException
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, CompositeStep
from .models import Province, Department
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.DEPARTMENTS, [
        utils.CheckDependenciesStep([Province]),
        extractors.DownloadURLStep(constants.DEPARTMENTS + '.zip',
                                   config.get('etl', 'departments_url'), constants.DEPARTMENTS),
        transformers.ExtractZipStep(
            internal_path=""
        ),
        loaders.Ogr2ogrStep(table_name=constants.DEPARTMENTS_TMP_TABLE,
                            geom_type='MultiPolygon',
                            env={'SHAPE_ENCODING': 'utf-8'}),
        utils.ValidateTableSchemaStep({
            'ogc_fid': 'integer',
            'gid': 'numeric',
            'objeto': 'varchar',
            'fna': 'varchar',
            'gna': 'varchar',
            'nam': 'varchar',
            'sag': 'varchar',
            'fdc': 'varchar',
            'in1': 'varchar',
            'geom': 'geometry'
        }),
        CompositeStep([
            DepartmentsExtractionStep(),
            utils.DropTableStep()
        ]),
        utils.FirstResultStep,
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'departments_target_size')),
        CompositeStepCreateFile(
            Department, 'departments', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('departments', config),
    ])


class DepartmentsExtractionStep(transformers.EntitiesExtractionStep):

    def __init__(self):
        super().__init__('departments_extraction', Department,
                         entity_class_pkey='id', tmp_entity_class_pkey='in1')

    def _patch_tmp_entities(self, tmp_departments, ctx):
        # Elasticsearch (georef-ar-api) no procesa correctamente la geometría
        # de algunos gobiernos locales, lanza un error "Self-intersection at or near point..."
        # Validar la geometría utilizando ST_MakeValid().
        def make_valid_geom(dep):
            sql_str = """
                                            select ST_MakeValid(geom)
                                            from {}
                                            where in1=:in1
                                            limit 1
                                            """.format(dep.__table__.name)

            # GeoAlchemy2 no disponibiliza la function ST_MakeValid, utilizar
            # SQL manualmente (como excepción).
            dep.geom = ctx.session.scalar(sql_str, {'in1': dep.in1})

        patch.apply_fn(tmp_departments, make_valid_geom, ctx, in1='94021')

    def _process_entity(self, tmp_department, cached_session, ctx):
        lon, lat = geometry.get_centroid_coordinates(tmp_department.geom,
                                                     ctx)
        dept_id = tmp_department.in1
        prov_id = dept_id[:constants.PROVINCE_ID_LEN]

        province = cached_session.query(Province).get(prov_id)
        if not province:
            raise ValidationException(
                'No existe la provincia con ID {}'.format(prov_id))

        if ctx.mode == 'normal':
            province_isct = geometry.get_intersection_percentage(
                province.geometria, tmp_department.geom, ctx)
        else:
            province_isct = 0  # Saltear operaciones costosas en testing

        return Department(
            id=dept_id,
            nombre=utils.clean_string(tmp_department.nam),
            nombre_completo=utils.clean_string(tmp_department.fna),
            categoria=utils.clean_string(tmp_department.gna),
            lon=lon, lat=lat,
            provincia_interseccion=province_isct,
            provincia_id=prov_id,
            fuente=utils.clean_string(tmp_department.sag),
            geometria=tmp_department.geom
        )
