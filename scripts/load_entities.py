import os
import psycopg2
from geo_admin.models import State, Department, Municipality, Settlement
from georef.settings import BASE_DIR


MESSAGES = {
    'states_info': '-- Cargando la entidad Provincia.',
    'states_success': 'Las provincias fueron cargadas exitosamente.',
    'states_error': 'Las provincias no pudieron cargarse.',
    'departments_info': '-- Cargando la entidad Departamento.',
    'departments_success': 'Los departamentos fueron cargados exitosamente.',
    'departments_error': 'Los departamentos no pudieron cargarse.',
    'departments_dependency_error': 'Deben cargarse las provincias antes de '
                                    'los departamentos.',
    'municipalities_info': '-- Cargando la entidad Municipalidad.',
    'municipalities_success': 'Los municipios fueron cargados exitosamente.',
    'municipalities_error': 'Los municipios no pudieron cargarse.',
    'municipalities_dependency_error': 'Deben cargarse provincias y '
                                       'departamentos antes de los municipios.',
    'settlements_info': '-- Cargando la entidad Asentamientos.',
    'settlements_success': 'Los asentamientos fueron cargados exitosamente.',
    'settlements_error': 'Los asentamientos no pudieron cargarse.',
    'settlements_dependency_error': 'Deben cargarse provincias y departamentos '
                                    'antes de los asentamientos.',
    'script_info': '-- Cargando script SQL.',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar el script SQL.'
}


def run():
    try:
        load_script('functions_load_entities.sql')
        replace_tables()
        load_script('ign_entities_patch.sql')
        state_ids = load_states()
        department_ids = load_departments(state_ids)
        municipality_ids = load_municipalities(state_ids, department_ids)
        load_settlements(state_ids, department_ids, municipality_ids)
    except Exception as e:
        print(e)


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return results
    except psycopg2.DatabaseError as e:
        print(e)


def replace_tables():
    query = "SELECT replace_tables()"
    run_query(query)


def load_script(file):
    try:
        print(MESSAGES['script_info'])
        files_path = BASE_DIR + '/etl_scripts/' + file

        with open(files_path, 'r') as f:
            func = f.read()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(func)
        print(MESSAGES['script_success'] % file)
    except psycopg2.DatabaseError as e:
        print("{0}: {1}".format(MESSAGES['script_error'], e))


def load_states():
    try:
        print(MESSAGES['states_info'])
        query = """SELECT in1 AS code, \
                          upper(nam) AS name, \
                          st_y(st_centroid(geom)) as lat, \
                          st_x(st_centroid(geom)) as lon, \
                          geom \
                    FROM  ign_provincias \
                    ORDER BY code;
                """
        states = run_query(query)
        states_list = []
        for row in states:
            (code, name, lat, lon, geom) = row
            states_list.append(State(code=code, name=name, lat=lat,
                                     lon=lon, geom=geom))
        State.objects.all().delete()
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
            print(MESSAGES['departments_info'])
            query = """SELECT in1 as code, \
                              upper(nam) as name, \
                              st_y(st_centroid(geom)) as lat, \
                              st_x(st_centroid(geom)) as lon, \
                              geom, \
                              substring(in1,1,2) as state_id \
                        FROM  ign_departamentos \
                        ORDER BY code;
                    """
            departments = run_query(query)
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
            print(MESSAGES['municipalities_info'])
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
            municipalities = run_query(query)
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


def load_settlements(state_ids, department_ids, municipality_ids):
    if state_ids and department_ids and municipality_ids:
        try:
            print(MESSAGES['settlements_info'])
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
            settlements = run_query(query)
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
