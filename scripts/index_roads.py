from elasticsearch import Elasticsearch
from geo_admin.models import Department, Locality, State, Road


def run():
    try:
        index_roads()
    except Exception as e:
        print(e)


def index_roads():
    es = Elasticsearch()
    print('-- Creando índices de Calles.')
    departments = {dept.code: dept.name for dept in Department.objects.all()}
    localities = {loc.id: loc.name for loc in Locality.objects.all()}
    for state in State.objects.all():
        index_name = '-'.join(['calles', state.code])
        if es.indices.exists(index=index_name):
            print('-- Ya existe el índice "%s".' % index_name)
            continue
        data = []
        for road in Road.objects.filter(state_id=state.id):
            document = {
                'nomenclatura': ', '.join([road.name, localities[road.locality_id], state.name]),
                'codigo': road.code,
                'nombre': road.name,
                'tipo': road.road_type,
                'inicio_derecha': road.start_right,
                'inicio_izquierda': road.start_left,
                'fin_derecha': road.end_right,
                'fin_izquierda': road.end_left,
                'geometria': road.geom,
                'codigo_postal': road.postal_code,
                'localidad': localities[road.locality_id],
                'departamento': departments[road.code[:5]],
                'provincia': state.name
            }
            data.append({'index': {'_id': road.id}})
            data.append(document)
        if data:
            es.bulk(index=index_name, doc_type='calle', body=data, refresh=True)
            print('-- Se creó el índice "%s".' % index_name)
