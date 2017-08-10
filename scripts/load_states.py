import csv
from georef.settings import BASE_DIR
from geo_admin.models import State


MESSAGES = {
    'success': 'Las provincias fueron cargadas exitosamente.',
    'load_error': 'Las provincias no pudieron cargarse'
}


def run():
    try:
        states = []
        with open(BASE_DIR + '/data/provincias.cs', newline='') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            for row in reader:
                states.append(State(code=row[0], name=row[1]))
        State.objects.bulk_create(states)
        print(MESSAGES['success'])
    except Exception as e:
        print("{0}: {1}".format(MESSAGES['load_error'], e))
