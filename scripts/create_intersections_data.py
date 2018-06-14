# -*- coding: utf-8 -*-

"""Módulo 'create_intersections_data' de georef-etl

Contiene funciones para la impresión de intersecciones procesadas desde
vías de intersección.
"""

import logging
import json
import psycopg2
import os
from datetime import datetime

MESSAGES = {
    'intersections_export_info': '-- Exportando datos de intersecciones',
    'intersections_export_success': 'Los datos de intersecciones para la '
                                    'Provincia con código "%s" fueron '
                                    'exportados exitosamente',
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
        create_intersections_data()
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
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except psycopg2.DatabaseError as e:
        logging.error(e)


def create_intersections_data():
    """Obtiene y genera datos de intersecciones de vías de circulación por cada
       entidad de Provincia.

    Returns:
        None
    """
    logging.info(MESSAGES['intersections_export_info'])
    entities_code = ['02', '06', '10', '14', '18', '22', '26', '30', '34', '38',
                     '42', '46', '50', '54', '58', '62', '66', '70', '74', '78',
                     '82', '86', '90', '94']

    query = """SELECT nomencla_a, nombre_a, tipo_a, departamento_a, provincia_a, 
                      nomencla_b, nombre_b, tipo_b, departamento_b, provincia_b,
                      ST_Y(geom) AS lat, ST_X(geom) AS lon
               FROM {}
            """

    for code in entities_code:
        data = []
        table_name = 'indec_intersecciones_{}'.format(code)
        intersections = run_query(query.format(table_name))
        for row in intersections:
            (nomencla_a, nombre_a, tipo_a, departamento_a, provincia_a,
             nomencla_b, nombre_b, tipo_b, departamento_b, provincia_b,
             lat, lon) = row
            data.append({
                'nomencla_a': nomencla_a,
                'nombre_a': nombre_a,
                'tipo_a': tipo_a,
                'departamento_a': departamento_a,
                'provincia_a': provincia_a,
                'nomencla_b': nomencla_b,
                'nombre_b': nombre_b,
                'tipo_b': tipo_b,
                'departamento_b': departamento_b,
                'provincia_b': provincia_b,
                'lat': lat,
                'lon': lon
            })
        create_data_file(code, data)
        logging.info(MESSAGES['intersections_export_success'] % code)


def create_data_file(code, data):
    """Imprime datos de intersecciones de vías de circulación por código de
       entidad en formato JSON.

    Args:
        code (str): Código de la entidad.
        data (list): Datos de la entidad a imprimir.

    Returns:
        None
    """
    filename = 'data/intersecciones/indec_intersecciones_{}.json' \
        .format(code)

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile, ensure_ascii=False)

