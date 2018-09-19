# -*- coding: utf-8 -*-

"""
Módulo 'load_entities' de georef-etl

Contiene funciones para popular la base de datos integrada con georef-etl desde
una base datos intermedia con entidades políticas (asentamientos, municipios,
departamentos y provincias).
"""

import logging
import os
import psycopg2
from datetime import datetime
from geo_admin.models import State, Department, Municipality, Settlement
from georef.settings import BASE_DIR


MESSAGES = {
    'entity_load_info': '-- Cargando la entidad %s',
    'entity_load_success': 'Los datos para la entidad %s fueron cargados '
                           'exitosamente.',
    'entity_load_error': 'Los datos para la entidad %s no pudieron cargarse.',
    'departments_dependency_error': 'Deben cargarse las provincias antes de '
                                    'los departamentos.',
    'municipalities_dependency_error': 'Deben cargarse provincias y '
                                       'departamentos antes de los municipios.',
    'settlements_dependency_error': 'Deben cargarse provincias y departamentos '
                                    'antes de los asentamientos.',
    'script_info': '-- Cargando script SQL',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar el script SQL.',
    'update_entities_info': '-- Reemplazando datos temporales',
    'update_entities_success': 'Se actualizaron los datos de las entidades.'
}


logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


def run():
    """Contiene las funciones a llamar cuando se ejecuta el script.

    Returns:
        None
    """
    try:
        load_script('functions_load_entities.sql')
        update_entities_data()
        load_script('ign_entities_patch.sql')
        state_ids = load_states()
        department_ids = load_departments(state_ids)
        municipality_ids = load_municipalities(state_ids, department_ids)
        load_settlements(state_ids, department_ids, municipality_ids)
    except Exception as e:
        logging.error(e)


def get_db_connection():
    """Se conecta a una base de datos especificada en variables de entorno.

    Returns:
        connection: Conexión a base de datos.
    """
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    """Procesa y ejecuta una consulta en la base de datos especificada.

    Args:
        query (str): Consulta a ejecutar.

    Returns:
        list: Resultado de la consulta.
    """
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return results
    except psycopg2.DatabaseError as e:
        logging.error(e)


def load_script(file):
    """Se conecta a la base datos especificada y realiza la carga de scripts SQL.

    Returns:
        None
    """
    try:
        logging.info(MESSAGES['script_info'])
        files_path = BASE_DIR + '/etl_scripts/' + file

        with open(files_path, 'r') as f:
            func = f.read()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(func)
        logging.info(MESSAGES['script_success'] % file)
    except psycopg2.DatabaseError as e:
        logging.error("{0}: {1}".format(MESSAGES['script_error'], e))


def update_entities_data():
    """Ejecuta la llamada de la función 'update_entities' que reemplaza los
      datos de la base de datos intermedia con los datos descargados desde los
      correspondientes portales.

    Returns:
        None
    """
    logging.info(MESSAGES['update_entities_info'])
    query = "SELECT update_entities_data()"
    run_query(query)
    logging.info(MESSAGES['update_entities_success'])


def load_states():
    """Ejecuta una consulta sobre la base de datos intermedia para obtener los
       datos de la entidad Provincia, e inserta los resultados en la base de
       datos integrada con georef-etl.

    Returns:
       dict: Diccionario con códigos de la entidad Provincia.
    """
    try:
        logging.info(MESSAGES['entity_load_info'] % 'Provincia')
        query = """SELECT in1 AS code, \
                          nam AS name, \
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
        logging.info(MESSAGES['entity_load_success'] % 'Provincia')
    except Exception as e:
        logging.error("{0}: {1}".format(MESSAGES['entity_load_error'] %
                                        'Provincia', e))
    finally:
        return {state.code: state.id for state in State.objects.all()}


def load_departments(state_ids):
    """Ejecuta una consulta sobre la base de datos intermedia para obtener los
       datos de la entidad Departamento, e inserta los resultados en la base de
       datos integrada con georef-etl.

    Returns:
       dict: Diccionario con códigos de la entidad Departamento.
    """
    if state_ids:
        caba = State.objects.get(code='02')
        Department.objects.get_or_create(name=caba.name, code='02000',
                                         state=caba, lat=0.0, lon=0.0)
        try:
            logging.info(MESSAGES['entity_load_info'] % 'Departamento')
            query = """SELECT in1 AS code, \
                              nam AS name, \
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
            logging.info(MESSAGES['entity_load_success'] % 'Departamento')
        except Exception as e:
            logging.error("{0}: {1}".format(MESSAGES['entity_load_error']
                                            % 'Departamento', e))
        finally:
            return {dept.code: dept.id for dept in Department.objects.all()}
    else:
        logging.error(MESSAGES['departments_dependency_error'])


def load_municipalities(state_ids, department_ids):
    """Ejecuta una consulta sobre la base de datos intermedia para obtener los
       datos de la entidad Municipio, e inserta los resultados en la base de
       datos integrada con georef-etl.

    Returns:
       dict: Diccionario con códigos de la entidad Municipio.
    """
    if state_ids:
        try:
            logging.info(MESSAGES['entity_load_info'] % 'Municipio')
            query = """SELECT in1 AS code, \
                               nam AS name, \
                               st_y(st_centroid(geom)) AS lat, \
                               st_x(st_centroid(geom)) AS lon, \
                               geom, \
                               substring(in1, 1, 2) AS state_id
                        FROM ign_municipios \
                        ORDER BY code;
                    """
            municipalities = run_query(query)
            municipalities_list = []
            for row in municipalities:
                code, name, lat, lon, geom, state_code = row
                municipalities_list.append(Municipality(
                    code=code,
                    name=name,
                    lat=lat,
                    lon=lon,
                    geom=geom,
                    state_id=state_ids[state_code]
                ))
            Municipality.objects.bulk_create(municipalities_list)
            logging.info(MESSAGES['entity_load_success'] % 'Municipio')
        except Exception as e:
            logging.error("{0}: {1}".format(MESSAGES['entity_load_error']
                                            % 'Municipio', e))
        finally:
            return {mun.code: mun.id for mun in Municipality.objects.all()}
    else:
        logging.error(MESSAGES['municipalities_dependency_error'])


def load_settlements(state_ids, department_ids, municipality_ids):
    """Ejecuta una consulta sobre la base de datos intermedia para obtener los
       datos de la entidad Asentamiento, e inserta los resultados en la base de
       datos integrada con georef-etl.

    Returns:
       dict: Diccionario con códigos de la entidad Asentamiento.
    """
    if state_ids and department_ids and municipality_ids:
        try:
            logging.info(MESSAGES['entity_load_info'] % 'Asentamiento')
            query = """SELECT cod_bahra AS code, \
                          nombre_bah AS name, \
                          tipo_bahra AS bahra_type, \
                          get_municipality(cod_bahra) AS municipality_id, \
                          cod_depto AS dep_code, \
                          cod_prov AS state_code, \
                          st_y(st_centroid(geom)) AS lat, \
                          st_x(st_centroid(geom)) AS lon, \
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
            logging.info(MESSAGES['entity_load_success'] % 'Asentamiento')
        except Exception as e:
            logging.error("{0}: {1}".format(MESSAGES['entity_load_error']
                                            % 'Asentamiento', e))
    else:
        logging.error(MESSAGES['settlements_dependency_error'])
