# -*- coding: utf-8 -*-

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
    'script_info': '-- Cargando script SQL.',
    'script_success': 'El script "%s" fue cargado exitosamente.',
    'script_error': 'Ocurrió un error al cargar el script SQL.'
}

logging.basicConfig(
    filename='logs/etl_intersecciones_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


def run():
    try:
        load_functions()
        create_intersections_table()
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error("{0}: {1}".format(MESSAGES['intersections_error'], e))


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)


def load_functions():
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


def create_intersections_table():
    logging.info(MESSAGES['intersections_create_info'])
    query = "SELECT process_intersections()"
    run_query(query)
    logging.info(MESSAGES['intersections_create_success'])
