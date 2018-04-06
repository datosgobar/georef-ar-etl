from georef.settings import BASE_DIR
from geo_admin.models import State, Department, Locality, Municipality, \
    Settlement
import csv
import os
import psycopg2


MESSAGES = {
    'states_success': 'Las provincias fueron cargadas exitosamente.',
    'states_error': 'Las provincias no pudieron cargarse.',
    'departments_success': 'Los departamentos fueron cargados exitosamente.',
    'departments_error': 'Los departamentos no pudieron cargarse.',
    'departments_dependency_error': 'Deben cargarse las provincias antes de '
                                    'los departamentos.',
    'localities_success': 'Las localidades fueron cargadas exitosamente.',
    'localities_error': 'Las localidades no pudieron cargarse.',
    'localities_dependency_error': 'Deben cargarse provincias y departamentos '
                                   'antes de las localidades.',
    'settlements_success': 'Los asentamientos fueron cargados exitosamente.',
    'settlements_error': 'Los asentamientos no pudieron cargarse.',
    'settlements_dependency_error': 'Deben cargarse provincias y departamentos '
                                   'antes de los asentamientos.',
    'municipalities_success': 'Los municipios fueron cargados exitosamente.',
    'municipalities_error': 'Los municipios no pudieron cargarse.',
    'municipalities_dependency_error': 'Deben cargarse provincias y '
                                       'departamentos antes de los municipios.',
    'functions_success': 'Las funciones SQL fueron cargadas exitosamente.',
    'functions_error': 'Ocurrió un error al cargar las funciones SQL.'
}


def run():
    try:
        load_functions()
        state_ids = load_states()
        department_ids = load_departments(state_ids)
        municipality_ids = load_municipalities(state_ids, department_ids)
        load_localities(state_ids, department_ids)
        load_settlements(state_ids, department_ids, municipality_ids)
    except Exception as e:
        print(e)


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def load_functions():
    try:
        print('-- Cargando funciones SQL.')

        files_path = [
            BASE_DIR + '/etl_scripts/ddl_tables.sql',
            BASE_DIR + '/etl_scripts/ign_entities_patch.sql',
            BASE_DIR + '/etl_scripts/function_get_department.sql',
            BASE_DIR + '/etl_scripts/function_get_municipality.sql',
        ]

        for file in files_path:
            with open(file, 'r') as f:
                func = f.read()
            with get_db_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(func)
        
        print(MESSAGES['functions_success'])
    except psycopg2.DatabaseError as e:
        print(MESSAGES['functions_error'])
        print(e)


def run_query_entities(query):
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            entities = cursor.fetchall()
        return entities
    except psycopg2.DatabaseError as e:
        print(e)


def load_states():
    try:
        print('-- Cargando la entidad Provincia.')
        query = """SELECT in1 AS code, \
                          upper(nam) AS name, \
                          st_y(st_centroid(geom)) as lat, \
                          st_x(st_centroid(geom)) as lon, \
                          geom \
                    FROM  ign_provincias \
                    ORDER BY code;
                """
        states = run_query_entities(query)
        states_list = []
        for row in states:
            (code, name, lat, lon, geom) = row
            states_list.append(State(code=code, name=name, lat=lat,
                                     lon=lon, geom=geom))
        State.objects.bulk_create(states_list)
        print(MESSAGES['states_success'])
    except Exception as e:
        print("{0}: {1}".format(MESSAGES['states_error'], e))
    finally:
        return {state.code: state.id for state in State.objects.all()}


def load_departments(state_ids):
    if state_ids:
        caba = State.objects.get(code='02')
        Department.objects.get_or_create(name=caba.name, code='02000',
                                         state=caba, lat=0.0, lon=0.0)
        try:
            print('-- Cargando la entidad Departamento.')
            query = """SELECT in1 as code, \
                              upper(nam) as name, \
                              st_y(st_centroid(geom)) as lat, \
                              st_x(st_centroid(geom)) as lon, \
                              geom, \
                              substring(in1,1,2) as state_id \
                        FROM  ign_departamentos \
                        ORDER BY code;
                    """
            departments = run_query_entities(query)
            departments_list = []
            for row in departments:
                (code, name, lat, lon, geom, state_code) = row
                departments_list.append(Department(
                    code=code,
                    name=name,
                    lat=lat,
                    lon=lon,
                    geom=geom,
                    state_id=state_ids[state_code]
                ))
            Department.objects.bulk_create(departments_list)
            print(MESSAGES['departments_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['departments_error'], e))
        finally:
            return {dept.code: dept.id for dept in Department.objects.all()}
    else:
        print(MESSAGES['departments_dependency_error'])


def load_municipalities(state_ids, department_ids):
    if state_ids:
        try:
            print('-- Cargando la entidad Municipalidad.')
            query = """SELECT in1 as code, \
                               upper(nam) as name, \
                               st_y(st_centroid(geom)) as lat, \
                               st_x(st_centroid(geom)) as lon, \
                               geom, \
                               get_department(in1) as department_id, \
                               substring(in1, 1, 2) as state_id \
                        FROM ign_municipios \
                        ORDER BY code;
                    """
            municipalities = run_query_entities(query)
            municipalities_list = []
            for row in municipalities:
                code, name, lat, lon, geom, dept_code, state_code = row
                municipalities_list.append(Municipality(
                    code=code,
                    name=name,
                    lat=lat,
                    lon=lon,
                    geom=geom,
                    department_id=department_ids[dept_code] or None,
                    state_id=state_ids[state_code]
                ))
            Municipality.objects.bulk_create(municipalities_list)
            print(MESSAGES['municipalities_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['municipalities_error'], e))
        finally:
            return {mun.code: mun.id for mun in Municipality.objects.all()}
    else:
        print(MESSAGES['municipalities_dependency_error'])


def load_localities(state_ids, department_ids):
    if state_ids and department_ids:
        localities = []
        try:
            print('-- Cargando la entidad Localidad.')
            file_path = BASE_DIR + '/data/localidades.csv'
            with open(file_path, newline='', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skips headers row.
                for row in reader:
                    loc_code, loc_name, state_code, dept_code = row
                    localities.append(Locality(
                        code=loc_code,
                        name=loc_name,
                        department_id=department_ids[dept_code],
                        state_id=state_ids[state_code]
                    ))
            Locality.objects.bulk_create(localities)
            print(MESSAGES['localities_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['localities_error'], e))
    else:
        print(MESSAGES['localities_dependency_error'])


def load_settlements(state_ids, department_ids, municipality_ids):
    if state_ids and department_ids and municipality_ids:
        try:
            print('-- Cargando la entidad Asentamientos.')
            query = """SELECT cod_bahra as code, \
                          upper(nombre_bah) as name, \
                          tipo_bahra as bahra_type, \
                          get_municipality(cod_bahra) as municipality_id, \
                          cod_depto as dep_code, \
                          cod_prov as state_code, \
                          st_y(st_centroid(geom)) as lat, \
                          st_x(st_centroid(geom)) as lon, \
                          geom \
                       FROM ign_bahra \
                       WHERE tipo_bahra IN ('LS', 'E', 'LC') \
                       ORDER BY code;
                    """
            settlements = run_query_entities(query)
            settlements_list = []
            for row in settlements:
                (code, name, bahra_type, mun_code, dep_code, state_code,
                 lat, lon, geom) = row
                settlements_list.append(Settlement(
                    code=code,
                    name=name,
                    bahra_type=bahra_type,
                    municipality_id=municipality_ids[mun_code] if mun_code else None,
                    department_id=department_ids[state_code + dep_code],
                    state_id=state_ids[state_code],
                    lat=lat,
                    lon=lon,
                    geom=geom
                ))
            Settlement.objects.bulk_create(settlements_list)
            print(MESSAGES['settlements_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['settlements_error'], e))
    else:
        print(MESSAGES['settlements_dependency_error'])
