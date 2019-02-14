from .etl import ETL
from .models import Province, Department
from . import extractors, transformers, loaders, geometry, utils, constants
from . import patch


class DepartmentsETL(ETL):
    def __init__(self):
        super().__init__(constants.DEPARTMENTS)

    def _run_internal(self, ctx):
        # Descargar el archivo de la URL
        url = ctx.config.get('etl', 'departments_url')
        filename = extractors.download_url(constants.DEPARTMENTS + '.zip', url,
                                           ctx)

        # Descomprimir el .zip
        zip_dir = transformers.extract_zipfile(filename, ctx)

        # Cargar el archivo .shp a la base de datos
        loaders.ogr2ogr(zip_dir, table_name=constants.DEPARTMENTS_RAW_TABLE,
                        geom_type='MultiPolygon', encoding='utf-8',
                        precision=True, ctx=ctx)

        # Crear una Table automáticamente a partir de la tabla generada por
        # ogr2ogr
        raw_departments = utils.automap_table(constants.DEPARTMENTS_RAW_TABLE,
                                              ctx)

        # Aplicar parche
        self._patch_raw_departments(raw_departments, ctx)

        # Leer la tabla raw_provinces para crear las entidades procesadas
        self._insert_clean_departments(raw_departments, ctx)

    def _patch_raw_departments(self, raw_departments, ctx):
        # Antártida Argentina duplicada
        patch.delete(raw_departments, ctx, ogc_fid=530, in1='94028')

        # Error de tipeo
        patch.update_field(raw_departments, 'in1', '54084', ctx, in1='55084')

        # Chascomús
        patch.update_field(raw_departments, 'in1', '06217', ctx, in1='06218')

        # Río Grande
        patch.update_field(raw_departments, 'in1', '94008', ctx, in1='94007')

        # Ushuaia
        patch.update_field(raw_departments, 'in1', '94015', ctx, in1='94014')

        # Tolhuin
        patch.update_field(raw_departments, 'in1', '94011', ctx,
                           fna='Departamento Río Grande', nam='Tolhuin')

    def _insert_clean_departments(self, raw_departments, ctx):
        departments = []

        # TODO: Manejar comparación con deptos que ya están en la base
        ctx.query(Department).delete()
        query = ctx.query(raw_departments)
        count = query.count()

        ctx.logger.info('Insertando departamentos procesados...')

        for raw_department in utils.pbar(query, ctx, total=count):
            lon, lat = geometry.get_centroid(raw_department, ctx)
            dept_id = raw_department.in1
            prov_id = dept_id[:constants.PROVINCE_ID_LEN]

            province = ctx.query(Province).get(prov_id)
            province_isct = geometry.get_intersection_percentage(
                province, raw_department, ctx, geom_field_a='geometria')

            department = Department(
                id=dept_id,
                nombre=utils.clean_string(raw_department.nam),
                nombre_completo=utils.clean_string(raw_department.fna),
                categoria=utils.clean_string(raw_department.gna),
                lon=lon, lat=lat,
                provincia_interseccion=province_isct,
                provincia_id=province.id,
                fuente=utils.clean_string(raw_department.sag),
                geometria=raw_department.geom
            )

            # TODO: Sistema que compruebe la integridad de los nuevos datos
            assert len(department.id) == constants.DEPARTMENT_ID_LEN

            departments.append(department)

        ctx.session.add_all(departments)
