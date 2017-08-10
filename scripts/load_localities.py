from georef.settings import BASE_DIR
from geo_admin.models import Department, Locality, State
import csv


MESSAGES = {
    'success': 'Las localidades fueron cargadas exitosamente.',
    'error': 'Las localidades no pudieron cargarse.'
}


def run():
    state_ids = {state.code: state.id for state in State.objects.all()}
    department_ids = {dept.code: dept.id for dept in Department.objects.all()}
    if state_ids and department_ids:
        localities = []
        try:
            file_path = BASE_DIR + '/data/localidades.csv'
            with open(file_path, newline='') as csv_file:
                reader = csv.reader(csv_file)
                next(reader) # Skips headers row.
                for row in reader:
                    loc_code, loc_name, state_code, dept_code = row
                    localities.append(Locality(
                        code=loc_code,
                        name=loc_name,
                        department_id=department_ids[dept_code],
                        state_id=state_ids[state_code]
                        ))
            Locality.objects.bulk_create(localities)
            print(MESSAGES['success'])
        except Exception as e:
            print("{0}: {1}".format(MESSAGES['error'], e))
    else:
        print('Deben cargarse provincias y departamentos antes de las localidades.')
