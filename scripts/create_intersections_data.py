# -*- coding: utf-8 -*-

"""Módulo 'create_intersections_data' de georef-etl

Contiene funciones para la impresión de intersecciones procesadas desde
vías de intersección.
"""

import logging
import psycopg2
import os
from datetime import datetime
from scripts.create_entities_data import add_metadata, create_data_file
from scripts.load_roads import SOURCE

MESSAGES = {
    'intersections_export_info': '-- Exportando datos de intersecciones',
    'intersections_export_lenght': 'Cantidad de intersecciones: %s'
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
    entities = []
    data = {}
    entities_code = ['02', '06', '10', '14', '18', '22', '26', '30', '34', '38',
                     '42', '46', '50', '54', '58', '62', '66', '70', '74', '78',
                     '82', '86', '90', '94', 'provincias']

    query = """SELECT a_nomencla, a_nombre, a_tipo, 
                      a_dept_id, a_dept_nombre, a_prov_id, a_prov_nombre, 
                      b_nomencla, b_nombre, b_tipo, 
                      b_dept_id, b_dept_nombre, b_prov_id, b_prov_nombre, 
                      ST_Y(geom) AS lat, ST_X(geom) AS lon
               FROM {}
            """

    for code in entities_code:
        table_name = 'indec_intersecciones_{}'.format(code)
        intersections = run_query(query.format(table_name))

        for row in intersections:
            (a_id, a_nom, a_tipo, a_dept_id, a_dept_nom, a_prov_id, a_prov_nom,
             b_id, b_nom, b_tipo, b_dept_id, b_dept_nom, b_prov_id, b_prov_nom,
             lat, lon) = row

            entities.append({
                'id': '-'.join([a_id, b_id]),
                'calle_a': {
                    'id': a_id,
                    'nombre': a_nom,
                    'departamento': {
                        'id': a_dept_id,
                        'nombre': a_dept_nom
                    },
                    'provincia': {
                        'id': a_prov_id,
                        'nombre': a_prov_nom,
                    },
                    'categoria': a_tipo,
                    'fuente': SOURCE
                },
                'calle_b': {
                    'id': b_id,
                    'nombre': b_nom,
                    'departamento': {
                        'id': b_dept_id,
                        'nombre': b_dept_nom
                    },
                    'provincia': {
                        'id': b_prov_id,
                        'nombre': b_prov_nom
                    },
                    'categoria': b_tipo,
                    'fuente': SOURCE
                },
                'geometria': {
                    'type': 'Point',
                    'coordinates': [
                        lon, lat
                    ]
                }
            })

    add_metadata(data)
    data['datos'] = entities
    logging.info(MESSAGES['intersections_export_lenght'] % len(entities))
    create_data_file('interseccion', 'json', data)
