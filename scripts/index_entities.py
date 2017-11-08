from elasticsearch import Elasticsearch
from geo_admin.models import Department, Locality, Settlement, State


def run():
    try:
        es = Elasticsearch()
        index_states(es)
        index_departments(es)
        index_localities(es)
        index_settlements(es)
    except Exception as e:
        print(e)


def index_states(es):
    if es.indices.exists(index='provincias'):
        print('-- Ya existe el índice de Provincias.')
        return
    print('-- Creando índice de Provincias.')
    data = []
    for state in State.objects.all():
        data.append({'index': {'_id': state.id}})
        data.append({'id': state.code, 'nombre': state.name})
    es.bulk(index='provincias', doc_type='provincia', body=data, refresh=True)
    print('-- Se creó el índice de Provincias exitosamente.')


def index_departments(es):
    if es.indices.exists(index='departamentos'):
        print('-- Ya existe el índice de Departamentos.')
        return
    print('-- Creando índice de Departamentos.')
    data = []
    states = {state.id: (state.code, state.name) for state in State.objects.all()}
    for dept in Department.objects.all():
        document = {
            'id': dept.code,
            'nombre': dept.name,
            'provincia': {
                'id': states[dept.state_id][0],
                'nombre': states[dept.state_id][1]
            }
        }
        data.append({'index': {'_id': dept.id}})
        data.append(document)
    es.bulk(index='departamentos', doc_type='departamento', body=data, refresh=True)
    print('-- Se creó el índice de Departamentos exitosamente.')


def index_localities(es):
    if es.indices.exists(index='localidades'):
        print('-- Ya existe el índice de Localidades.')
        return
    print('-- Creando índice de Localidades.')
    data = []
    states = {state.id: (state.code, state.name) for state in State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in Department.objects.all()}
    for locality in Locality.objects.all():
        document = {
            'id': locality.code,
            'nombre': locality.name,
            'departamento': {
                'id': departments[locality.department_id][0],
                'nombre': departments[locality.department_id][1]
            },
            'provincia': {
                'id': states[locality.state_id][0],
                'nombre': states[locality.state_id][1]
            }
        }
        data.append({'index': {'_id': locality.id}})
        data.append(document)
    es.bulk(index='localidades', doc_type='localidad', body=data, refresh=True)
    print('-- Se creó el índice de Localidades exitosamente.')


def index_settlements(es):
    if es.indices.exists(index='bahra'):
        print('-- Ya existe el índice de BAHRA.')
        return
    print('-- Creando índice de Asentamientos.')
    barha_types = {
        'E': 'Entidad (E)',
        'LC': 'Componente de localidad compuesta (LC)',
        'LS': 'Localidad simple (LS)'
    }
    data = []
    states = {state.id: (state.code, state.name) for state in State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in Department.objects.all()}
    for settlement in Settlement.objects.all():
        document = {
            'id': settlement.code,
            'nombre': settlement.name,
            'tipo': barha_types[settlement.bahra_type],
            'departamento': {
                'id': departments[settlement.department_id][0],
                'nombre': departments[settlement.department_id][1]
            },
            'provincia': {
                'id': states[settlement.state_id][0],
                'nombre': states[settlement.state_id][1]
            },
            'ubicacion': {
                'lat': settlement.lat,
                'lon': settlement.lon
            }
        }
        data.append({'index': {'_id': settlement.id}})
        data.append(document)
    es.bulk(index='bahra', doc_type='asentamiento', body=data, refresh=True)
    print('-- Se creó el índice de Asentamientos exitosamente.')
