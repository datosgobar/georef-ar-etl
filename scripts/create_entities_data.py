# -*- coding: utf-8 -*-

"""Módulo 'create_entities_data' de georef-etl

Contiene funciones para la impresión de datos correspondientes a entidades
políticas (asentamientos, municipios, departamentos y provincias) en formato
JSON.
"""

import logging
import json
import os
import subprocess
from datetime import datetime
from datetime import timezone
from geo_admin.models import State, Department, Municipality, Settlement


MESSAGES = {
    'entity_info_get': '-- Obteniendo de datos de la entidad %s',
    'entity_info_generate': '-- Creando datos de la entidad %s',
    'entity_succes_generate': 'Los datos de la entidad %s fueron creados '
                              'exitosamente.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


version = subprocess.check_output('git describe --always --dirty --tag',
                                  shell=True, encoding="utf-8").rstrip('\n')


def run():
    """Contiene las funciones a llamar cuando se ejecuta el script.

    Returns:
        None
    """
    try:
        create_data_states()
        create_data_departments()
        create_data_municipalities()
        create_data_settlements()
    except Exception as e:
        logging.error(e)


def create_data_states():
    """Obtiene y genera datos de la entidad Provincia.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_get'] % '{}'.format('Provincia'))
    data = {}
    entities = []

    for state in State.objects.all():
        entities.append({
            'id': state.code,
            'nombre': state.name,
            'lat': str(state.lat),
            'lon': str(state.lon),
            'geometria': {
                'type': 'multipolygon',
                'coordinates': state.geom.coords
            }
        })

    add_metadata(data)
    data['entidades'] = entities
    create_data_file('provincia', data)


def create_data_departments():
    """Obtiene y genera datos de la entidad Departamento.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_get'] % '{}'.format('Departamento'))
    data = {}
    entities = []

    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}

    for dept in Department.objects.all():
        geometry = {}
        if dept.geom is not None:
            geometry = {'type': 'multipolygon', 'coordinates': dept.geom.coords}
        entities.append({
            'id': dept.code,
            'nombre': dept.name,
            'lat': str(dept.lat),
            'lon': str(dept.lon),
            'geometria': geometry,
            'provincia': {
                'id': states[dept.state_id][0],
                'nombre': states[dept.state_id][1]
            }
        })

    add_metadata(data)
    data['entidades'] = entities
    create_data_file('departamento', data)


def create_data_municipalities():
    """Obtiene y genera datos de la entidad Municipio.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_get'] % '{}'.format('Municipio'))
    data = {}
    entities = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in
                   Department.objects.all()}

    for mun in Municipality.objects.all():
        entities.append({
            'id': mun.code,
            'nombre': mun.name,
            'lat': str(mun.lat),
            'lon': str(mun.lon),
            'geometria': {
                'type': 'multipolygon',
                'coordinates': mun.geom.coords,
            },
            'departamento': {
                'id': departments[mun.department_id][0],
                'nombre': departments[mun.department_id][1]
            },
            'provincia': {
                'id': states[mun.state_id][0],
                'nombre': states[mun.state_id][1]
            }
        })

    add_metadata(data)
    data['entidades'] = entities
    create_data_file('municipio', data)


def create_data_settlements():
    """Obtiene y genera datos de la entidad Asentamiento.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_get'] % '{}'.format('Asentamiento'))
    bahra_types = {
        'E': 'Entidad (E)',
        'LC': 'Componente de localidad compuesta (LC)',
        'LS': 'Localidad simple (LS)'
    }

    data = {}
    entities = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in
                   Department.objects.all()}
    municipalities = {mun.id: (mun.code, mun.name)
                      for mun in Municipality.objects.all()}

    for settlement in Settlement.objects.all():
        entities.append({
            'id': settlement.code,
            'nombre': settlement.name,
            'tipo': bahra_types[settlement.bahra_type],
            'lat': str(settlement.lat),
            'lon': str(settlement.lon),
            'geometria': {
                'type': 'multipoint',
                'coordinates': settlement.geom.coords
            },
            'municipio': {
                'id': municipalities[settlement.municipality_id][0]
                if settlement.municipality_id else None,
                'nombre': municipalities[settlement.municipality_id][1]
                if settlement.municipality_id else None
            },
            'departamento': {
                'id': departments[settlement.department_id][0],
                'nombre': departments[settlement.department_id][1]
            },
            'provincia': {
                'id': states[settlement.state_id][0],
                'nombre': states[settlement.state_id][1]
            }
        })

    add_metadata(data)
    data['entidades'] = entities
    create_data_file('asentamiento', data)


def add_metadata(data):
    """Agrega metadatos (fecha, commit) a un diccionario con
    datos de entidades.

    Args:
        data (dic): Diccionario que contiene una lista de entidades.
    """
    now = datetime.now(timezone.utc)
    data['fecha_actualizacion'] = str(now)
    data['timestamp'] = now.timestamp()
    data['version'] = version



def create_data_file(entity, data):
    """Imprime datos de una entidad en formato JSON.

    Args:
        entity (str): Nombre de la entidad.
        data (dic): Datos de la entidad a imprimir.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_generate'] % '{}'.format(entity.title()))
    filename = 'data/entidades/{}s.json'.format(entity)

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile, ensure_ascii=False)
    logging.info(MESSAGES['entity_succes_generate'] % '{}'.
                 format(entity.title()))
