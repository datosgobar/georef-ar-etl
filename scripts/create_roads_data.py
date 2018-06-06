# -*- coding: utf-8 -*-

import logging
import json
import os
from datetime import datetime
from geo_admin.models import Department, State, Road


MESSAGES = {
    'roads_data_info': '-- Creando datos de Calles.',
    'roads_state_info': '-- Cargando datos para: %s, cantidad: %d',
    'road_data_success': 'Los datos de "%s" fueron creados correctamente.',
    'road_data_error': 'Los datos de "%s" no pudieron ser creados.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


def run():
    try:
        index_roads()
    except Exception as e:
        logging.error(e)


def index_roads():
    logging.info(MESSAGES['roads_data_info'])
    departments = {dept.id: dept.name for dept in Department.objects.all()}
    
    for state in State.objects.all():
        index_name = '-'.join(['calles', state.code])
        data = []
        roads_filtered = Road.objects.filter(state_id=state.id)
        
        roads_count = roads_filtered.count()
        logging.info(MESSAGES['roads_state_info'] % (index_name, roads_count))

        for road in roads_filtered:
            document = {
                'nomenclatura': ', '.join([
                    road.name,
                    departments[road.dept_id],
                    state.name]),
                'id': road.code,
                'nombre': road.name,
                'tipo': road.road_type,
                'inicio_derecha': road.start_right,
                'inicio_izquierda': road.start_left,
                'fin_derecha': road.end_right,
                'fin_izquierda': road.end_left,
                'geometria': road.geom,
                'codigo_postal': road.postal_code,
                'departamento': departments[road.dept_id],
                'provincia': state.name
            }
            data.append(document)

        filename = 'data/vias/{}.json'

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename.format(index_name), 'w') as outfile:
            json.dump(data, outfile)
        if data:
            logging.info(MESSAGES['road_data_success'] % index_name)
        else:
            logging.error(MESSAGES['road_data_error'] % index_name)
