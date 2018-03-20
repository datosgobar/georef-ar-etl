from elasticsearch import Elasticsearch
from geo_admin.models import Department, Locality, State, Road

from scripts.elasticsearch_params import DEFAULT_SETTINGS,\
    LOWCASE_ASCII_NORMALIZER, NAME_ANALYZER, NAME_ANALYZER_ROAD_SYNONYMS,\
    NAME_ANALYZER_ENTITY_SYNONYMS


STREET_MAPPING = {
    'calle': {
        'properties': {
            'nomenclatura': {
                'type': 'text',
                'index': False
            },
            'id': {'type': 'keyword'},
            'nombre': {
                'type': 'text',
                'analyzer': NAME_ANALYZER_ROAD_SYNONYMS,
                'search_analyzer': NAME_ANALYZER,
                'fields': {
                    'exacto': {
                        'type': 'keyword',
                        'normalizer': LOWCASE_ASCII_NORMALIZER
                    }
                }
            },
            'tipo': {
                'type': 'text',
                'analyzer': NAME_ANALYZER_ROAD_SYNONYMS,
                'search_analyzer': NAME_ANALYZER
            },
            'inicio_derecha': {
                'type': 'integer',
                'index': False
            },
            'inicio_izquierda': {
                'type': 'integer',
                'index': False
            },
            'fin_derecha': {
                'type': 'integer',
                'index': False
            },
            'fin_izquierda': {
                'type': 'integer',
                'index': False
            },
            'geometria': {
                'type': 'text',
                'index': False
            },
            'codigo_postal': {
                'type': 'text',
                'index': False
            },
            'localidad': {
                'type': 'text',
                'analyzer': NAME_ANALYZER_ENTITY_SYNONYMS,
                'search_analyzer': NAME_ANALYZER,
                'fields': {
                    'exacto': {
                        'type': 'keyword',
                        'normalizer': LOWCASE_ASCII_NORMALIZER
                    }
                }
            },
            'provincia': {
                'type': 'text',
                'analyzer': NAME_ANALYZER_ENTITY_SYNONYMS,
                'search_analyzer': NAME_ANALYZER,
                'fields': {
                    'exacto': {
                        'type': 'keyword',
                        'normalizer': LOWCASE_ASCII_NORMALIZER
                    }
                }
            },
            'departamento': {
                'type': 'text',
                'analyzer': NAME_ANALYZER_ENTITY_SYNONYMS,
                'search_analyzer': NAME_ANALYZER,
                'fields': {
                    'exacto': {
                        'type': 'keyword',
                        'normalizer': LOWCASE_ASCII_NORMALIZER
                    }
                }               
            }
        }
    }
}


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
                'id': road.code,
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
            es.indices.create(index=index_name, body={
                'settings': DEFAULT_SETTINGS,
                'mappings': STREET_MAPPING
            })
            es.bulk(index=index_name, doc_type='calle', body=data,
                    refresh=True, request_timeout=320)
            print('-- Se creó el índice "%s".' % index_name)
