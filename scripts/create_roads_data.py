from geo_admin.models import Department, Locality, State, Road
import json
import os


MESSAGES = {
    'roads_data_info': '-- Creando datos de Calles.',
    'road_data_success': 'Los datos de "%s" fueron creados correctamente.',
    'road_data_error': 'Los datos de "%s" no pudieron ser creados.'
}


def run():
    try:
        index_roads()
    except Exception as e:
        print(e)


def index_roads():
    print(MESSAGES['roads_data_info'])
    departments = {dept.code: dept.name for dept in Department.objects.all()}
    localities = {loc.id: loc.name for loc in Locality.objects.all()}
    for state in State.objects.all():
        index_name = '-'.join(['calles', state.code])
        data = []
        for road in Road.objects.filter(state_id=state.id):
            document = {
                'nomenclatura': ', '.join([
                    road.name,
                    localities[road.locality_id],
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
                'localidad': localities[road.locality_id],
                'departamento': departments[road.code[:5]],
                'provincia': state.name
            }
            data.append({'index': {'_id': road.id}})
            data.append(document)

        filename = 'data/vias/{}.json'

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename.format(index_name), 'w') as outfile:
            json.dump(data, outfile)
        if data:
            print(MESSAGES['road_data_success'] % index_name)
        else:
            print(MESSAGES['road_data_error'] % index_name)
