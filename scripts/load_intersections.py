# -*- coding: utf-8 -*-

"""Módulo 'load_intersections' de georef-etl

Contiene funciones para popular la base de datos integrada con georef-etl desde
una base datos intermedia, con intersecciones procesadas a partir de vías de
intersección.

"""

import logging
import psycopg2
import os
from datetime import datetime
from georef.settings import BASE_DIR


MESSAGES = {
    'intersections_create_info': '-- Creando datos de intersecciones',
    'intersections_create_success': 'Los datos de intersecciones fueron creados'
                                    ' exitosamente.',
    'intersections_error': 'Se produjo un error al crear los datos de '
                           'intersecciones.',
    'script_info': '-- Cargando script SQL',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar el script SQL.'
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
        load_functions()
        create_intersections()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error("{0}: {1}".format(MESSAGES['intersections_error'], e))


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
    except psycopg2.DatabaseError as e:
        logging.error(e)


def load_functions():
    """Se conecta a la base datos especificada y realiza la carga de scripts SQL.

    Returns:
        None
    """
    try:
        logging.info(MESSAGES['script_info'])
        file = BASE_DIR + '/etl_scripts/functions_intersections.sql'

        with open(file, 'r') as f:
            func = f.read()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(func)
        logging.info(MESSAGES['script_success'] % file)
    except psycopg2.DatabaseError as e:
        logging.error("{0}: {1}".format(MESSAGES['script_error'], e))


def create_intersections():
    """Ejecuta la llamada de la función 'process_intersections' para el proceso
      de elaboración de intersecciones a partir de los datos de vías de
      intersección.

    Returns:
        None
    """
    logging.info(MESSAGES['intersections_create_info'])
    query = "SELECT process_intersections()"
    run_query(query)
    logging.info(MESSAGES['intersections_create_success'])
