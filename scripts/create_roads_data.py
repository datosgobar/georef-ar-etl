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

            document = {
                'nomenclatura': ', '.join([
                    road.name,
                    dept.name,
                    state.name]),
                'id': road.code,
                'nombre': road.name,
                'tipo': road.road_type,
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
                'geometria': road.geom,
                'departamento': {
                    'id': dept.code,
                    'nombre': dept.name
                },
                'provincia': {
                    'id': state.code,
                    'nombre': state.name
                }
            }
            roads.append(document)

    create_entities_data.add_metadata(data)
    data['datos'] = roads

    create_data_file('json', data)
    create_data_file('csv', flatten_list(roads))


def create_data_file(file_format, data):
    filenames = [
        'data/{}/calles.{}'.format(version, file_format),
        'data/latest/calles.{}'.format(file_format)
    ]

    for filename in filenames:
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename, 'w') as outfile:
            if file_format in 'csv':
                keys = data[0].keys()
                dict_writer = csv.DictWriter(outfile, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            else:
                json.dump(data, outfile, ensure_ascii=False)

    if data:
        logging.info(MESSAGES['road_data_success'] % file_format)
    else:
        logging.error(MESSAGES['road_data_error'])


def flatten_list(data):
    """Aplana un lista.

    Args:
        data (dic): Datos de la entidad a imprimir.

    Returns:
        entities (list): Resultado aplanado.
    """
    entities = []
    for row in data:
        entities.append(flatten_dict(row))
    return entities


def flatten_dict(dd, separator='_', prefix=''):
    """Aplana un diccionario.

    Args:
        dd (dic): Diccionario a aplanar.
        separator (str): Separador entre palabras.
        prefix (str): Prefijo.

    Returns:
        dict: Diccionario aplanado.
    """
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}

