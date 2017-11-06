import csv
from georef.settings import BASE_DIR
from geo_admin.models import State, Department, Locality


MESSAGES = {
    'states_success': 'Las provincias fueron cargadas exitosamente.',
    'states_error': 'Las provincias no pudieron cargarse.',
    'departments_success': 'Los departamentos fueron cargados exitosamente.',
    'departments_error': 'Los departamentos no pudieron cargarse.',
    'localities_success': 'Las localidades fueron cargadas exitosamente.',
    'localities_error': 'Las localidades no pudieron cargarse.',
    'localities_dependency_error': 'Deben cargarse provincias y departamentos '
                                   'antes de las localidades.'
}


def run():
    try:
        load_states()
        load_departments()
        load_localities()
    except Exception as e:
        print(e)


def load_states():
    try:
        states = []
        file_path = BASE_DIR + '/data/provincias.csv'
        with open(file_path, newline='') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            for row in reader:
                states.append(State(code=row[0], name=row[1]))
        State.objects.bulk_create(states)
        print(MESSAGES['states_success'])
    except Exception as e:
        print("{0}: {1}".format(MESSAGES['states_error'], e))


def load_departments():
    state_ids = {state.code: state.id for state in State.objects.all()}
    if state_ids:
        caba = State.objects.get(code='02')
        Department.objects.create(name=caba.name, code='02000', state=caba)
        departments = []
        try:
            file_path = BASE_DIR + '/data/departamentos.csv'
            with open(file_path, newline='') as csv_file:
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
    else:
        print('Deben cargarse las provincias antes de los departamentos.')


def load_localities():
    state_ids = {state.code: state.id for state in State.objects.all()}
    department_ids = {dept.code: dept.id for dept in Department.objects.all()}
    if state_ids and department_ids:
        localities = []
        try:
            file_path = BASE_DIR + '/data/localidades.csv'
            with open(file_path, newline='') as csv_file:
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
