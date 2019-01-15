# -*- coding: utf-8 -*-

"""
Módulo 'load_entities' de georef-etl

Contiene funciones para popular la base de datos integrada con georef-etl desde
una base datos intermedia con entidades políticas (asentamientos, municipios,
departamentos y provincias).
"""

import csv
import logging
import os
import psycopg2
from datetime import datetime
from geo_admin.models import *
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
        load_countries()
        state_ids = load_states()
        department_ids = load_departments(state_ids)
        municipality_ids = load_municipalities(state_ids)
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


def load_countries():
    """Ejecuta una consulta sobre la base de datos intermedia para obtener los
       datos de la entidad País, e inserta los resultados en la base de
       datos integrada con georef-etl.

    Returns:
       None
    """
    try:
        logging.info(MESSAGES['entity_load_info'] % 'País')
        query = """SELECT fna AS name, \
                          nam AS name_sort, \
                          gna AS category, \
                          sag AS source, \
                          st_y(st_centroid(geom)) AS lat, \
                          st_x(st_centroid(geom)) AS lon, \
                          geom \
                    FROM  ign_pais \
                    ORDER BY name;
                """
        countries = run_query(query)
        countries_list = []
        for row in countries:
            (name, name_short, category, source, lat, lon, geom) = row
            code, iso_name, iso_code = add_data_iso_country(name)

            countries_list.append(Country(name=name, name_short=name_short,
                                          code=code, iso_code=iso_code,
                                          iso_name=iso_name, category=category,
                                          source=source, lat=lat, lon=lon,
                                          geom=geom))

        Country.objects.all().delete()
        Country.objects.bulk_create(countries_list)
        logging.info(MESSAGES['entity_load_success'] % 'País')
    except Exception as e:
        logging.error("{0}: {1}".format(MESSAGES['entity_load_error'] %
                                        'País', e))


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
                          fna AS name, \
                          nam AS name_short, \
                          gna AS category, \
                          sag AS source, \
                          st_y(st_centroid(geom)) AS lat, \
                          st_x(st_centroid(geom)) AS lon, \
                          geom \
                    FROM  ign_provincias \
                    ORDER BY code;
                """
        states = run_query(query)
        arg = Country.objects.get(name='República Argentina')
        states_list = []
        for row in states:
            (code, name, name_short, category, source, lat, lon, geom) = row
            iso_code, iso_name = add_data_iso_state(code)

            states_list.append(State(code=code, name=name, name_short=name_short,
                                     iso_code=iso_code, iso_name=iso_name,
                                     country_id=arg.id, category=category,
                                     source=source, lat=lat, lon=lon, geom=geom))

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
        try:
            logging.info(MESSAGES['entity_load_info'] % 'Departamento')
            query = """SELECT in1 AS code, \
                              fna AS name, \
                              nam AS name_short, \
                              gna AS category, \
                              sag AS source, \
                              substring(in1, 1, 2) AS state_id, \
                              get_percentage_intersection_state(geom,
                              substring(in1, 1, 2)) AS intersection, \
                              st_y(st_centroid(geom)) AS lat, \
                              st_x(st_centroid(geom)) AS lon, \
                              geom \
                        FROM  ign_departamentos \
                        ORDER BY code;
                    """
            departments = run_query(query)
            departments_list = []
            for row in departments:
                (code, name, name_short, category, source, state_id,
                 intersection, lat, lon, geom) = row
                departments_list.append(Department(
                    code=code,
                    name=name,
                    name_short=name_short,
                    category=category,
                    source=source,
                    state_id=state_ids[state_id],
                    state_intersection=intersection,
                    lat=lat,
                    lon=lon,
                    geom=geom
                ))

            Department.objects.all().delete()

            caba = State.objects.get(code='02')  # consistencia con bahra
            Department.objects.get_or_create(code='02000', name=caba.name,
                                             name_short=caba.name, state=caba,
                                             state_intersection=caba.code,
                                             lat=caba.lat, lon=caba.lon,
                                             geom=caba.geom)

            Department.objects.bulk_create(departments_list)
            logging.info(MESSAGES['entity_load_success'] % 'Departamento')
        except Exception as e:
            logging.error("{0}: {1}".format(MESSAGES['entity_load_error']
                                            % 'Departamento', e))
        finally:
            return {dept.code: dept.id for dept in Department.objects.all()}
    else:
        logging.error(MESSAGES['departments_dependency_error'])


def load_municipalities(state_ids):
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
                               fna AS name, \
                               nam AS name_short, \
                               gna AS category, \
                               sag AS source, \
                               substring(in1, 1, 2) AS state_id, \
                               get_percentage_intersection_state(geom,
                               substring(in1, 1, 2)) AS intersection, \
                               st_y(st_centroid(geom)) AS lat, \
                               st_x(st_centroid(geom)) AS lon, \
                               geom \
                        FROM ign_municipios \
                        ORDER BY code;
                    """
            municipalities = run_query(query)
            municipalities_list = []
            for row in municipalities:
                (code, name, name_short, category, source, state_id,
                 intersection, lat, lon, geom) = row
                municipalities_list.append(Municipality(
                    code=code,
                    name=name,
                    name_short=name_short,
                    category=category,
                    source=source,
                    state_id=state_ids[state_id],
                    state_intersection=intersection,
                    lat=lat,
                    lon=lon,
                    geom=geom
                ))

            Municipality.objects.all().delete()
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
                          get_municipality(cod_bahra) AS municipality_id, \
                          cod_depto AS department_id, \
                          cod_prov AS state_id, \
                          tipo_bahra AS category, \
                          fuente_ubi AS source, \
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
                (code, name, mun_code, dep_code, state_code, category, source,
                 lat, lon, geom) = row
                settlements_list.append(Settlement(
                    code=code,
                    name=name,
                    municipality_id=municipality_ids[mun_code] if mun_code else None,
                    department_id=department_ids[state_code + dep_code],
                    state_id=state_ids[state_code],
                    category=category,
                    source=source,
                    lat=lat,
                    lon=lon,
                    geom=geom
                ))

            Settlement.objects.all().delete()
            Settlement.objects.bulk_create(settlements_list)
            logging.info(MESSAGES['entity_load_success'] % 'Asentamiento')
        except Exception as e:
            logging.error("{0}: {1}".format(MESSAGES['entity_load_error']
                                            % 'Asentamiento', e))
    else:
        logging.error(MESSAGES['settlements_dependency_error'])


def add_data_iso_state(code):
    # TODO: Agregar docstring
    file_path = BASE_DIR + '/georef/iso-3166-provincias-arg.csv'
    csv_file = csv.reader(open(file_path, "r"), delimiter=",")
    for row in csv_file:
        if code == row[0]:
            return row[2], row[3]
    # TODO: Mejorar respuesta
    return None, None


def add_data_iso_country(name):
    # TODO: Agregar docstring
    file_path = BASE_DIR + '/georef/iso-alfa-3-paises.csv'
    csv_file = csv.reader(open(file_path, "r"), delimiter=",")
    for row in csv_file:
        if name == row[1]:
            return row[0], row[2], row[3]
    # TODO: Mejorar respuesta
    return None, None, None
