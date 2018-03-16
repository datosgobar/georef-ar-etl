from geo_admin.models import Department, Locality, State, Road
import json


MESSAGES = {
    'roads_data_info': '-- Creando datos de Calles.',
    'roads_data_success': '-- Los datos de "%s" fueron creados correctamente',
    'roads_process_success': '-- Los datos fueron creados correctamente'
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

        with open('{}.json'.format(index_name), 'w') as outfile:
            json.dump(data, outfile)
        if data:
            print(MESSAGES['roads_data_success'] % index_name)

    print(MESSAGES['roads_process_success'])
