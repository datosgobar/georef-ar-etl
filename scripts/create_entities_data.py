from geo_admin.models import State, Department, Municipality, Locality,\
    Settlement
import json

MESSAGES = {
    'states_info': '-- Creando datos de la entidad Provincia.',
    'states_success': '-- Los datos de la entidad Provincia '
                      'fueron creados exitosamente.',
    'departments_info': '-- Creando datos de la entidad Departamento.',
    'departments_success': '-- Los datos de la entidad Departamento '
                           'fueron creados exitosamente.',
    'municipality_info': '-- Creando datos de la entidad Municipalidad.',
    'municipality_success': '-- Los datos de la entidad Municipalidad '
                            'fueron creados exitosamente.',
    'locality_info': '-- Creando datos de la entidad Localidad.',
    'locality_sucess': '-- Los datos de la entidad Localidad '
                       'fueron creados exitosamente.',
    'settlement_info': '-- Creando datos de la entidad Asentamiento.',
    'settlement_success': '-- Los datos de la entidad Asentamiento '
                          'fueron creados exitosamente.'
}


def run():
    try:
        create_data_states()
        create_data_departments()
        create_data_municipalities()
        create_data_localities()
        create_data_settlements()
    except Exception as e:
        print(e)


def create_data_states():
    print(MESSAGES['states_info'])
    data = []
    for state in State.objects.all():
        data.append({'index': {'_id': state.id}})
        data.append({
            'id': state.code,
            'nombre': state.name,
            'lat': str(state.lat),
            'lon': str(state.lon),
            'geometry': {
                'type': 'polygon',
                'coordinates': state.geom.coords
            }
        })
    with open('provincias.json', 'w') as outfile:
        json.dump(data, outfile)
    print(MESSAGES['states_success'])


def create_data_departments():
    print(MESSAGES['departments_info'])
    data = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    for dept in Department.objects.all():
        geometry = {}
        if dept.geom is not None:
            geometry = {'type': 'polygon', 'coordinates': dept.geom.coords}
        data.append({'index': {'_id': dept.id}})
        data.append({
            'id': dept.code,
            'nombre': dept.name,
            'lat': str(dept.lat),
            'lon': str(dept.lon),
            'geometry': geometry,
            'provincia': {
                'id': states[dept.state_id][0],
                'nombre': states[dept.state_id][1]
            }
        })
    with open('departamentos.json', 'w') as outfile:
        json.dump(data, outfile)
    print(MESSAGES['departments_success'])


def create_data_municipalities():
    print(MESSAGES['municipality_info'])
    data = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in
                   Department.objects.all()}
    for mun in Municipality.objects.all():
        data.append({'index': {'_id': mun.id}})
        data.append({
            'id': mun.code,
            'nombre': mun.name,
            'lat': str(mun.lat),
            'lon': str(mun.lon),
            'geometry': {
                'type': 'polygon',
                'coordinates': mun.geom.coords,
            },
            'departamento': {
                'id': departments[mun.department_id][0],
                'nombre': departments[mun.department_id][1]
            },
            'provincia': {
                'id': states[mun.state_id][0],
                'nombre': states[mun.state_id][1]
            }
        })
    with open('municipalidades.json', 'w') as outfile:
        json.dump(data, outfile)
    print(MESSAGES['municipality_success'])


def create_data_localities():
    print(MESSAGES['locality_info'])
    data = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in
                   Department.objects.all()}
    for locality in Locality.objects.all():
        data.append({'index': {'_id': locality.id}})
        data.append({
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
        })
    with open('localidades.json', 'w') as outfile:
        json.dump(data, outfile)
    print(MESSAGES['locality_sucess'])


def create_data_settlements():
    print(MESSAGES['settlement_info'])
    bahra_types = {
        'E': 'Entidad (E)',
        'LC': 'Componente de localidad compuesta (LC)',
        'LS': 'Localidad simple (LS)'
    }

    data = []
    states = {state.id: (state.code, state.name) for state in
              State.objects.all()}
    departments = {dept.id: (dept.code, dept.name) for dept in
                   Department.objects.all()}

    for settlement in Settlement.objects.all():
        data.append({'index': {'_id': settlement.id}})
        data.append({
            'id': settlement.code,
            'nombre': settlement.name,
            'tipo': bahra_types[settlement.bahra_type],
            'lat': str(settlement.lat),
            'lon': str(settlement.lon),
            'geometry': {
                'type': 'multipoint',
                'coordinates': settlement.geom.coords
            },
            'departamento': {
                'id': departments[settlement.department_id][0],
                'nombre': departments[settlement.department_id][1]
            },
            'provincia': {
                'id': states[settlement.state_id][0],
                'nombre': states[settlement.state_id][1]
            }
        })
    with open('asentamientos.json', 'w') as outfile:
        json.dump(data, outfile)
    print(MESSAGES['settlement_success'])
