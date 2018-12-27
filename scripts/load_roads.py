# -*- coding: utf-8 -*-

"""Módulo 'load_roads' de georef-etl

Contiene funciones para popular la base de datos integrada con georef-etl desde
una base datos intermedia con datos de vías de circulación.

"""

import logging
import json
import psycopg2
import os
from geo_admin.models import Road, State, Department
from datetime import datetime


roads = []
flagged_roads = []
states = {state.code: state.id for state in State.objects.all()}
depts = {dept.code: dept.id for dept in Department.objects.all()}


logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


def run():
    """Contiene las funciones a llamar cuando se ejecuta el script.

    Returns:
        None
    """
    logging.info('-- Procesando vías --')
    try:
        streets = run_query()
        for row in streets:
            process_street(row)

        logging.info('-- Removiendo vías anteriores --')
        Road.objects.all().delete()

        logging.info('-- Insertando vías --')
        Road.objects.bulk_create(roads)

        logging.info('-- Proceso completo --')
        generate_report()
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


def run_query():
    """Ejecuta una consulta sobre la base de datos intermedia para obtener las
       vías de circulación.

    Returns:
        streets (dict): Diccionario con vías de circulación.
    """
    query = """SELECT nomencla as code, \
                    nombre as name, \
                    tipo as road_type, \
                    desdei as start_left, \
                    desded as start_right, \
                    hastai as end_left, \
                    hastad as end_right, \
                    geom \
            FROM  indec_vias \
            WHERE tipo <> 'OTRO';
            """
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            streets = cursor.fetchall()
        return streets
    except psycopg2.DatabaseError as e:
        logging.error(e)


def process_street(row):
    """Procesa y valida cada vía de circulación para insertar, en caso de cumplir
       con las condiciones dadas, en la base de datos integrada con georef-etl.

    Args:
        row: Objeto con información de una vía de circulación.

    Returns:
        None
    """
    (code, name, road_type, start_left, start_right,
        end_left, end_right, geom) = row
    
    obs = {}
    if name == 'CALLE S N':
        obs['nombre'] = 'Sin registro'
    flagged_boundaries = validate_boundaries(
        start_left, start_right, end_left, end_right)
    if flagged_boundaries:
        obs['alturas'] = flagged_boundaries

    state_code = code[:2]
    dept_code = code[:5]

    if state_code in states and dept_code in depts:
        state_id = states[state_code]
        dept_id = depts[dept_code]

        road = Road(code=code, name=name, road_type=road_type,
                    start_left=start_left, start_right=start_right,
                    end_left=end_left, end_right=end_right, geom=geom,
                    dept_id=dept_id, state_id=state_id)
        roads.append(road)
    else:
        if state_code not in states:
            obs['provincia'] = 'Provincia {} no encontrada'.format(state_code)
        if dept_code not in depts:
            obs['departamento'] = 'Depto. {} no encontrado'.format(dept_code)

    if obs:
        flagged_roads.append({
            'nombre': name, 'nomencla': code, 'obs': obs})


def validate_boundaries(start_left, start_right, end_left, end_right):
    """Procesa y valida las alturas de una vías de circulación para obtener
       metrícas sobre la fidelidad del dato.

    Args:
        start_left (str): Altura inicial lado izquierdo.
        start_right (str): Altura inicial lado derecho.
        end_left (str): Altura final lado izquierdo.
        end_right (str): Altura final lado derecho.

    Returns:
        obs (list): Resultado del proceso.
    """
    obs = []
    if start_left == start_right:
        obs.append('Derecha e izquierda iniciales coinciden: %s' % start_left)
    if end_left == end_right:
        obs.append('Derecha e izquierda finales coiciden: %s' % end_left)
    if start_left == end_left:
        obs.append('Inicial y final izquierdas coinciden: %s' % end_left)
    if start_right == end_right:
        obs.append('Inicial y final derechas coinciden: %s' % end_right)
    return obs


def generate_report():
    """Genera reporte e imprime en formato JSON el proceso de carga de vías de
       circulación.

    Return:
        None
    """
    timestamp = datetime.now().strftime('%d-%m-%Y a las %H:%M:%S')
    heading = 'Proceso ETL de datos INDEC ejecutado el %s.\n' % timestamp
    ok_roads_msg = '-- Calles procesadas exitosamente: %s' % len(roads)
    failed_roads_msg = '-- Calles con errores: %s' % len(flagged_roads)

    with open('logs/etl_{:%Y%m%d}.log'.format(datetime.now()), 'a') as report:
        logging.info('-- Generando reporte --')
        report.write(heading)
        report.write(ok_roads_msg + '\n')
        report.write(failed_roads_msg + '\n\n')

    if flagged_roads:
        logging.info('-- Generando log de errores --')
        with open('logs/flagged_roads.json', 'w') as report:
            json.dump(flagged_roads, report, indent=2)

    logging.info('** Resultado del proceso **')
    logging.info(ok_roads_msg)
    logging.info(failed_roads_msg)
