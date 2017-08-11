from elasticsearch import Elasticsearch
from geo_admin.models import Department, Locality, State


def run():
    try:
        index_states()
        index_departments()
        index_localities()
    except Exception as e:
        print(e)
    print('-- Se crearon los índices exitosamente.')


def index_states():
    print('-- Creando índice de Provincias.')
    data = []
    for state in State.objects.all():
        data.append({'index': {'_id': state.id}})
        data.append({'id': state.code, 'nombre': state.name})
    es = Elasticsearch()
    es.bulk(index='provincias', doc_type='provincia', body=data, refresh=True)


def index_departments():
    print('-- Creando índice de Departamentos.')
    data = []
    states = {state.id: state.name for state in State.objects.all()}
    for dept in Department.objects.all():
        document = {
            'id': dept.code,
            'nombre': dept.name,
            'provincia': {
                'id': dept.state_id,
                'nombre': states[dept.state_id]
            }
        }
        data.append({'index': {'_id': dept.id}})
        data.append(document)
    es = Elasticsearch()
    es.bulk(index='departamentos', doc_type='departamento', body=data, refresh=True)


def index_localities():
    print('-- Creando índice de Localidades.')
    data = []
    states = {state.id: state.name for state in State.objects.all()}
    departments = {dept.id: dept.name for dept in Department.objects.all()}
    for locality in Locality.objects.all():
        document = {
            'id': locality.code,
            'nombre': locality.name,
            'departamento': {
                'id': locality.department_id,
                'nombre': departments[locality.department_id]
            },
            'provincia': {
                'id': locality.state_id,
                'nombre': states[locality.state_id]
            }
        }
        data.append({'index': {'_id': locality.id}})
        data.append(document)
    es = Elasticsearch()
    es.bulk(index='localidades', doc_type='localidad', body=data, refresh=True)
