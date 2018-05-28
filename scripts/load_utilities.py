import psycopg2
import os
from georef.settings import BASE_DIR


MESSAGES = {
    'functions_info': '-- Cargando funciones SQL.',
    'functions_success': 'Las funciones SQL fueron cargadas exitosamente.',
    'functions_error': 'Ocurrió un error al cargar las funciones SQL.'
}


def run():
    try:
        load_utilities()
    except (Exception, psycopg2.DatabaseError) as e:
        raise ("{}: {}".format(print(MESSAGES['functions_error']), e))


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def load_utilities():
    print(MESSAGES['functions_info'])

    files_path = [
        BASE_DIR + '/etl_scripts/functions_entities_report.sql',
        BASE_DIR + '/etl_scripts/function_states.sql',
        BASE_DIR + '/etl_scripts/function_departments.sql',
        BASE_DIR + '/etl_scripts/function_municipalities.sql',
        BASE_DIR + '/etl_scripts/function_intersection.sql',
    ]

    for file in files_path:
        with open(file, 'r') as f:
            func = f.read()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(func)

    print(MESSAGES['functions_success'])
