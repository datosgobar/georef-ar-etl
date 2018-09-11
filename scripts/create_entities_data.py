# -*- coding: utf-8 -*-

"""Módulo 'create_entities_data' de georef-etl

Contiene funciones para la impresión de datos correspondientes a entidades
políticas (asentamientos, municipios, departamentos y provincias) en formato
JSON.
"""

import csv
import geojson
import json
import logging
import os
import subprocess
from datetime import datetime
from datetime import timezone
from geo_admin.models import State, Department, Municipality, Settlement


MESSAGES = {
    'entity_info_get': '-- Obteniendo de datos de la entidad %s',
    'entity_info_generate': '-- Creando datos de la entidad %s',
    'entity_succes_generate': 'Los datos de la entidad %s en formato %s fueron '
                              'creados exitosamente.'
}

logging.basicConfig(
    filename='logs/etl_{:%Y%m%d}.log'.format(datetime.now()),
    level=logging.DEBUG, datefmt='%H:%M:%S',
    format='%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s')


version = subprocess.check_output('git describe --abbrev=0 --tags',
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
            'centroide': {
                'lat': float(state.lat),
                'lon': float(state.lon)
            },
            'geometria': {
                'type': 'multipolygon',
                'coordinates': state.geom.coords
            },
            'fuente': 'IGN'
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('provincia', 'json', data)
    create_data_file('provincia', 'csv', flatten_list('provincia', entities))
    create_data_file('provincia', 'geojson', convert_to_geojson(entities))


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
            'centroide': {
                'lat': float(dept.lat),
                'lon': float(dept.lon)
            },
            'geometria': geometry,
            'provincia': {
                'id': states[dept.state_id][0],
                'nombre': states[dept.state_id][1]
            },
            'fuente': 'IGN'
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('departamento', 'json', data)
    create_data_file('departamento', 'csv', flatten_list('departamento',
                                                         entities))
    create_data_file('departamento', 'geojson', convert_to_geojson(entities))


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
            'centroide': {
                'lat': float(mun.lat),
                'lon': float(mun.lon)
            },
            'geometria': {
                'type': 'multipolygon',
                'coordinates': mun.geom.coords,
            },
            'departamento': {
                'id': departments[mun.department_id][0],
                'nombre': departments[mun.department_id][1]
                # TODO: Re-incorporar información de áreas cuando se resuelva
                # el issue 104 de georef-ar-etl.
                # 'porcentaje_area_municipio': mun.department_area_percentage
            },
            'provincia': {
                'id': states[mun.state_id][0],
                'nombre': states[mun.state_id][1]
            },
            'fuente': 'IGN'
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('municipio', 'json', data)
    create_data_file('municipio', 'csv', flatten_list('municipio', entities))
    create_data_file('municipio', 'geojson', convert_to_geojson(entities))


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
            'centroide': {
                'lat': float(settlement.lat),
                'lon': float(settlement.lon)
            },
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
            },
            'fuente': 'BAHRA'
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('localidad', 'json', data)
    create_data_file('localidad', 'csv', flatten_list('localidad', entities))
    create_data_file('localidad', 'geojson', convert_to_geojson(entities))


def add_metadata(data):
    """Agrega metadatos (fecha, commit) a un diccionario con
    datos de entidades.

    Args:
        data (dict): Diccionario que contiene una lista de entidades.
    """
    now = datetime.now(timezone.utc)
    data['fecha_actualizacion'] = str(now)
    data['timestamp'] = int(now.timestamp())
    data['version'] = version


def create_data_file(entity, file_format, data):
    """Imprime datos de una entidad en un formato específico.

    Args:
        entity (str): Nombre de la entidad.
        file_format (str): Formato del archivo a imprimir.
        data (list,  dict): Datos de la entidad.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_generate'] % '{}'.format(entity.title()))
    filenames = [
        'data/{}/{}.{}'.format(version, plural_entity_level(entity), file_format),
        'data/latest/{}.{}'.format(plural_entity_level(entity), file_format)
    ]
    for filename in filenames:
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename, 'w', newline='') as outfile:
            if file_format in 'csv':
                keys = data[0].keys()
                dict_writer = csv.DictWriter(outfile, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            else:
                json.dump(data, outfile, ensure_ascii=False)

    logging.info(MESSAGES['entity_succes_generate'] %
                 ('{}'.format(entity.title()), file_format))


def plural_entity_level(entity_level):
    """Pluraliza el nombre de una unidad territorial.

    Args:
        entity_level (str): Nivel de la unidad territorial a pluralizar.

    Return:
        entity_level (str): Nombre pluralizado.
    """
    if 'localidad' not in entity_level:
        entity_level = entity_level + 's'
    else:
        entity_level = entity_level + 'es'
    return entity_level


def flatten_list(entity, data):
    """Aplana un lista y le agrega como prefijo el nombre de la entidad.

    Args:
        entity (str): Nombre de la entidad.
        data (list): Datos de la entidad a imprimir.

    Returns:
        entities (list): Resultado aplanado.
    """
    entities = []
    for row in data:
        row.pop('geometria')
        entities.append(flatten_dict(row, prefix=entity))
    return entities


def flatten_dict(dd, separator='_', prefix=''):
    """Aplana un diccionario.

    Args:
        dd (dict): Diccionario a aplanar.
        separator (str): Separador entre palabras.
        prefix (str): Prefijo.

    Returns:
        dict: Diccionario aplanado.
    """
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}


def convert_to_geojson(data):
    """Convierte un diccionario con datos de una entidad a formato geojson.

    Args:
        data (list): Diccionario con datos de la unidad territorial.

    Returns:
        entities (list): Resultado en formato geojson.
    """
    features = []
    for item in data:
        lat = item['centroide']['lat']
        lon = item['centroide']['lon']
        item.pop('centroide')

        point = geojson.Point((lat, lon))
        features.append(geojson.Feature(geometry=point, properties=item))
    return geojson.FeatureCollection(features)
