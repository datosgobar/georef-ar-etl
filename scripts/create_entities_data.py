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
import shutil
from datetime import datetime
from datetime import timezone
from geo_admin.models import *


MESSAGES = {
    'entity_info_get': '-- Obteniendo de datos de la entidad %s',
    'entity_info_generate': '-- Creando datos de la entidad %s',
    'entity_succes_generate': 'Los datos de la entidad %s en formato %s fueron'
                              ' creados exitosamente.'
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
        create_data_countries()
        create_data_states()
        create_data_departments()
        create_data_municipalities()
        create_data_settlements()
    except Exception as e:
        logging.error(e)


def create_data_countries():
    """Obtiene y genera datos de la entidad País.

    Returns:
        None
    """
    logging.info(MESSAGES['entity_info_get'] % '{}'.format('País'))
    data = {}
    entities = []

    for country in Country.objects.all():
        entities.append({
            'id': country.code,
            'nombre': country.name_short,
            'nombre_completo': country.name,
            'iso_id': country.iso_code,
            'iso_nombre': country.iso_name,
            'categoria': country.category,
            'fuente': country.source,
            'centroide': {
                'lat': float(country.lat),
                'lon': float(country.lon)
            },
            'geometria': {
                'type': 'MultiPolygon',
                'coordinates': country.geom.coords
            }
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('pais', 'json', data)
    create_data_file('pais', 'csv', flatten_list(entities, 'pais'))
    create_data_file('pais', 'geojson', convert_to_geojson(entities))


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
            'nombre': state.name_short,
            'nombre_completo': state.name,
            'iso_id': state.iso_code,
            'iso_nombre': state.iso_name,
            'categoria': state.category,
            'fuente': state.source,
            # TODO: Habilitar este campo cuando se generen los datos de paises
            # TODO: Cambiar campo a objeto con ID y nombre
            # 'pais': state.country.name,
            'centroide': {
                'lat': float(state.lat),
                'lon': float(state.lon)
            },
            'geometria': {
                'type': 'MultiPolygon',
                'coordinates': state.geom.coords
            }
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('provincia', 'json', data)
    create_data_file('provincia', 'csv', flatten_list(entities, 'provincia'))
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
            geometry = {
                'type': 'MultiPolygon',
                'coordinates': dept.geom.coords
            }

        entities.append({
            'id': dept.code,
            'nombre': dept.name_short,
            'nombre_completo': dept.name,
            'categoria': dept.category,
            'fuente': dept.source,
            'provincia': {
                'id': states[dept.state_id][0],
                'nombre': states[dept.state_id][1],
                'interseccion': dept.state_intersection
            },
            'centroide': {
                'lat': float(dept.lat),
                'lon': float(dept.lon)
            },
            'geometria': geometry
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('departamento', 'json', data)
    create_data_file('departamento', 'csv', flatten_list(entities,
                                                         'departamento'))
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

    for mun in Municipality.objects.all():
        entities.append({
            'id': mun.code,
            'nombre': mun.name_short,
            'nombre_completo': mun.name,
            'categoria': mun.category,
            'fuente': mun.source,
            'provincia': {
                'id': states[mun.state_id][0],
                'nombre': states[mun.state_id][1],
                'interseccion': mun.state_intersection
            },
            'centroide': {
                'lat': float(mun.lat),
                'lon': float(mun.lon)
            },
            'geometria': {
                'type': 'MultiPolygon',
                'coordinates': mun.geom.coords,
            },
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('municipio', 'json', data)
    create_data_file('municipio', 'csv', flatten_list(entities, 'municipio'))
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
            'categoria': bahra_types[settlement.category],
            'fuente': settlement.source,
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
            'centroide': {
                'lat': float(settlement.lat),
                'lon': float(settlement.lon)
            },
            'geometria': {
                'type': 'MultiPoint',
                'coordinates': settlement.geom.coords
            }
        })

    add_metadata(data)
    data['datos'] = entities
    create_data_file('localidad', 'json', data)
    create_data_file('localidad', 'csv', flatten_list(entities, 'localidad'))
    create_data_file('localidad', 'geojson', convert_to_geojson(entities))


def add_metadata(data):
    """Agrega metadatos (fecha, commit) a un diccionario con
    datos de entidades.

    Args:
        data (dict): Diccionario que contiene una lista de entidades.
    """
    now = datetime.now(timezone.utc)
    data['fecha_creacion'] = str(now)
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
    logging.info(MESSAGES['entity_info_generate'] % '{}'.format(
        entity.title()))

    filename = 'data/{}/{}.{}'.format(version, plural_entity_level(entity),
                                      file_format)

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, 'w', newline='') as outfile:
        if file_format in 'csv':
            keys = data[0].keys()
            dict_writer = csv.DictWriter(outfile, keys,
                                         quoting=csv.QUOTE_NONNUMERIC)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        else:
            json.dump(data, outfile, ensure_ascii=False)

    latest_fiename = 'data/latest/{}.{}'.format(plural_entity_level(entity),
                                                file_format)

    os.makedirs(os.path.dirname(latest_fiename), exist_ok=True)
    shutil.copyfile(filename, latest_fiename)

    logging.info(MESSAGES['entity_succes_generate'] %
                 ('{}'.format(entity.title()), file_format))


def plural_entity_level(entity_level):
    """Pluraliza el nombre de una unidad territorial.

    Args:
        entity_level (str): Nivel de la unidad territorial a pluralizar.

    Return:
        entity_level (str): Nombre pluralizado.
    """
    if entity_level in ['interseccion', 'localidad', 'pais']:
        entity_level = entity_level + 'es'
    else:
        entity_level = entity_level + 's'
    return entity_level


def flatten_list(data, entity=''):
    """Aplana un lista y agrega opcionalmente como prefijo el nombre de la
    entidad.

    Args:
        data (list): Datos de la entidad a imprimir.
        entity (str): Nombre de la entidad.

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
        lon = item['centroide']['lon']
        lat = item['centroide']['lat']
        item.pop('centroide')

        point = geojson.Point((lon, lat))
        features.append(geojson.Feature(geometry=point, properties=item))
    return geojson.FeatureCollection(features)
