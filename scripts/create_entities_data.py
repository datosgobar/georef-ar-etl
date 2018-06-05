# -*- coding: utf-8 -*-

import logging
import json
import os
from datetime import datetime
from geo_admin.models import State, Department, Municipality, Settlement


MESSAGES = {
    'states_info': '-- Creando datos de la entidad Provincia.',
    'states_success': 'Los datos de la entidad Provincia '
                      'fueron creados exitosamente.',
    'departments_info': '-- Creando datos de la entidad Departamento.',
    'departments_success': 'Los datos de la entidad Departamento '
                           'fueron creados exitosamente.',
    'municipality_info': '-- Creando datos de la entidad Municipio.',
    'municipality_success': 'Los datos de la entidad Municipio '
                            'fueron creados exitosamente.',
    'settlement_info': '-- Creando datos de la entidad Asentamiento.',
    'settlement_success': 'Los datos de la entidad Asentamiento '
                          'fueron creados exitosamente.'
}

logging.basicConfig(
    filename='logs/etl_entidades_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')


def run():
    try:
        create_data_states()
        create_data_departments()
        create_data_municipalities()
        create_data_settlements()
    except Exception as e:
        logging.error(e)


def create_data_states():
    logging.info(MESSAGES['states_info'])
    data = []
    entities = []
    data.append({'fecha_creacion': str(datetime.now())})

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
    data.append({'entidades': entities})

    filename = 'data/entidades/provincias.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile, ensure_ascii=False)
    logging.info(MESSAGES['states_success'])


def create_data_departments():
    logging.info(MESSAGES['departments_info'])
    data = []
    entities = []
    data.append({'fecha_creacion': str(datetime.now())})
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
    data.append({'entidades': entities})

    filename = 'data/entidades/departamentos.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile)
    logging.info(MESSAGES['departments_success'])


def create_data_municipalities():
    logging.info(MESSAGES['municipality_info'])
    data = []
    entities = []
    data.append({'fecha_creacion': str(datetime.now())})
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
    data.append({'entidades': entities})

    filename = 'data/entidades/municipios.json'
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile, ensure_ascii=False)
    logging.info(MESSAGES['municipality_success'])


def create_data_settlements():
    logging.info(MESSAGES['settlement_info'])
    bahra_types = {
        'E': 'Entidad (E)',
        'LC': 'Componente de localidad compuesta (LC)',
        'LS': 'Localidad simple (LS)'
    }

    data = []
    entities = []
    data.append({'fecha_creacion': str(datetime.now())})
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
    data.append({'entidades': entities})

    filename = 'data/entidades/asentamientos.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as outfile:
        json.dump(data, outfile, ensure_ascii=False)
    logging.info(MESSAGES['settlement_success'])
