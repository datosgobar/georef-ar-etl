from georef.settings import BASE_DIR
from geo_admin.models import Department, State
import csv


MESSAGES = {
    'success': 'Los departamentos fueron cargados exitosamente.',
    'error': 'Los departamentos no pudieron cargarse.'
}


def run():
    state_ids = {state.code: state.id for state in State.objects.all()}
    if state_ids:
        caba = State.objects.get(code='02')
        Department.objects.create(name=caba.name, code='02000', state=caba)
        departments = []
        try:
            file_path = BASE_DIR + '/data/departamentos.csv'
            with open(file_path, newline='') as csv_file:
                reader = csv.reader(csv_file)
                next(reader) # Skips headers row.
                for row in reader:
                    dept_code, dept_name, state_code = row
                    departments.append(Department(
                        code=dept_code,
                        name=dept_name,
                        state_id=state_ids[state_code]
                        ))
            Department.objects.bulk_create(departments)
            print(MESSAGES['success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['error'], e))
    else:
        print('Deben cargarse las provincias antes de los departamentos.')
