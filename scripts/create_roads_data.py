# -*- coding: utf-8 -*-

"""Módulo 'create_roads_data' de georef-etl

Contiene funciones para la impresión de datos correspondientes a vías de
circulación.
"""

import csv
import json
import logging
import os
import subprocess
from scripts import create_entities_data
from datetime import datetime
from geo_admin.models import Department, State, Road


MESSAGES = {
    'roads_data_info': '-- Creando datos de vías de circulación',
    'roads_state_info': '-- Cargando datos de vías para: %s, cantidad: %d',
    'road_data_success': 'Los datos de vías en formato %s fueron creados '
                         'correctamente.',
    'road_data_error': 'Los datos de vías no pudieron ser creados.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


version = subprocess.check_output('git describe --abbrev=0 --tags',
                                  shell=True, encoding="utf-8").rstrip('\n')


def run():
    """ Contiene las funciones a llamar cuando se ejecuta el script

    Returns:
        None
    """
    try:
        index_roads()
    except Exception as e:
        logging.error(e)


def index_roads():
    logging.info(MESSAGES['roads_data_info'])
    departments = {dept.id: dept for dept in Department.objects.all()}

    data = {}
    roads = []

    for state in State.objects.all():
        roads_filtered = Road.objects.filter(state_id=state.id)
        roads_count = roads_filtered.count()
        logging.info(MESSAGES['roads_state_info'] % (state.name, roads_count))

        for road in roads_filtered:
            dept = departments[road.dept_id]
            lines = [list(line) for line in road.geom]

            document = {
                'nomenclatura': ', '.join([
                    road.name,
                    dept.name,
                    state.name]),
                'id': road.code,
                'nombre': road.name,
                'categoria': road.category,
                'fuente': road.source,
                'altura': {
                    'inicio': {
                        'derecha': road.start_right,
                        'izquierda': road.start_left
                    },
                    'fin': {
                        'derecha': road.end_right,
                        'izquierda': road.end_left
                    }
                },
                # TODO: Habilitar campo cuando se generen los datos de
                # localidades censales.
                # TODO: Cambiar campo a objeto (ID y nombre).
                # 'localidad_id': road.locality_code,
                'departamento': {
                    'id': dept.code,
                    'nombre': dept.name_short
                },
                'provincia': {
                    'id': state.code,
                    'nombre': state.name_short
                },
                'geometria': {
                    'type': 'MultiLineString',
                    'coordinates': lines
                },
            }
            roads.append(document)

    create_entities_data.add_metadata(data)
    data['datos'] = roads

    create_entities_data.create_data_file('calle', 'json', data)
    create_entities_data.create_data_file('calle', 'csv',
                                          create_entities_data.flatten_list(
                                              roads))
