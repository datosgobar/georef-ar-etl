from elasticsearch import Elasticsearch
from geo_admin.models import Department, Locality, State


def run():
    try:
        es = Elasticsearch()
        index_states(es)
        index_departments(es)
        index_localities(es)
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
