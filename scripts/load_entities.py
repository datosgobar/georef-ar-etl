import csv
from georef.settings import BASE_DIR
from geo_admin.models import State, Department, Locality, Municipality, \
    Settlement


MESSAGES = {
    'states_success': 'Las provincias fueron cargadas exitosamente.',
    'states_error': 'Las provincias no pudieron cargarse.',
    'departments_success': 'Los departamentos fueron cargados exitosamente.',
    'departments_error': 'Los departamentos no pudieron cargarse.',
    'departments_dependency_error': 'Deben cargarse las provincias antes de '
                                    'los departamentos.',
    'localities_success': 'Las localidades fueron cargadas exitosamente.',
    'localities_error': 'Las localidades no pudieron cargarse.',
    'localities_dependency_error': 'Deben cargarse provincias y departamentos '
                                   'antes de las localidades.',
    'settlements_success': 'Los asentamientos fueron cargados exitosamente.',
    'settlements_error': 'Los asentamientos no pudieron cargarse.',
    'settlements_dependency_error': 'Deben cargarse provincias y departamentos '
                                   'antes de los asentamientos.',
    'municipalities_success': 'Los municipios fueron cargados exitosamente.',
    'municipalities_error': 'Los municipios no pudieron cargarse.',
    'municipalities_dependency_error': 'Deben cargarse provincias y '
                                       'departamentos antes de los municipios.'
}


def run():
    try:
        state_ids = load_states()
        department_ids = load_departments(state_ids)
        load_municipalities(state_ids)
        load_localities(state_ids, department_ids)
        load_settlements(state_ids, department_ids)
    except Exception as e:
        print(e)


def load_states():
    try:
        states = []
        file_path = BASE_DIR + '/data/provincias.csv'
        with open(file_path, newline='', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            for row in reader:
                states.append(State(code=row[0], name=row[1]))
        State.objects.bulk_create(states)
        print(MESSAGES['states_success'])
    except Exception as e:
        print("{0}: {1}".format(MESSAGES['states_error'], e))
    finally:
        return {state.code: state.id for state in State.objects.all()}


def load_departments(state_ids):
    if state_ids:
        caba = State.objects.get(code='02')
        Department.objects.get_or_create(name=caba.name, code='02000', state=caba)
        departments = []
        try:
            file_path = BASE_DIR + '/data/departamentos.csv'
            with open(file_path, newline='', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skips headers row.
                for row in reader:
                    dept_code, dept_name, state_code = row
                    departments.append(Department(
                        code=dept_code,
                        name=dept_name,
                        state_id=state_ids[state_code]
                        ))
            Department.objects.bulk_create(departments)
            print(MESSAGES['departments_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['departments_error'], e))
        finally:
            return {dept.code: dept.id for dept in Department.objects.all()}
    else:
        print(MESSAGES['departments_dependency_error'])


def load_municipalities(state_ids):
    if state_ids:
        municipalities = []
        try:
            file_path = BASE_DIR + '/data/municipios.csv'
            with open(file_path, newline='', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skips headers row.
                for row in reader:
                    mun_name, mun_code, state_code, lat, lon = row
                    municipalities.append(Municipality(
                        name=mun_name,
                        code=mun_code,
                        state_id=state_ids[state_code],
                        lat=lat,
                        lon=lon
                        ))
            Municipality.objects.bulk_create(municipalities)
            print(MESSAGES['municipalities_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['municipalities_error'], e))
    else:
        print(MESSAGES['municipalities_dependency_error'])


def load_localities(state_ids, department_ids):
    if state_ids and department_ids:
        localities = []
        try:
            file_path = BASE_DIR + '/data/localidades.csv'
            with open(file_path, newline='', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skips headers row.
                for row in reader:
                    loc_code, loc_name, state_code, dept_code = row
                    localities.append(Locality(
                        code=loc_code,
                        name=loc_name,
                        department_id=department_ids[dept_code],
                        state_id=state_ids[state_code]
                    ))
            Locality.objects.bulk_create(localities)
            print(MESSAGES['localities_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['localities_error'], e))
    else:
        print(MESSAGES['localities_dependency_error'])


def load_settlements(state_ids, department_ids):
    if state_ids and department_ids:
        settlements = []
        try:
            file_path = BASE_DIR + '/data/bahra.csv'
            with open(file_path, newline='', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                next(reader)  # Skips headers row.
                for row in reader:
                    (bahra_name, state_code, dept_code, loc_code,
                     bahra_code, bahra_type, lat, lon) = row
                    settlements.append(Settlement(
                        code=bahra_code,
                        name=bahra_name,
                        bahra_type=bahra_type,
                        department_id=department_ids[state_code + dept_code],
                        state_id=state_ids[state_code],
                        lat=lat,
                        lon=lon
                    ))
            Settlement.objects.bulk_create(settlements)
            print(MESSAGES['settlements_success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['settlements_error'], e))
    else:
        print(MESSAGES['settlements_dependency_error'])
