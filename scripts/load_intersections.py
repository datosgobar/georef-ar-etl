import psycopg2
import os


MESSAGES = {
    'intersections_create_info': '-- Creando datos de intersecciones',
    'intersections_create_success': 'Los datos de intersecciones fueron creados'
                                    ' exitosamente.',
    'intersections_error': 'Se produjo un error al crear los datos de '
                           'intersecciones.'
}


def run():
    try:
        create_intersections_table()
    except (Exception, psycopg2.DatabaseError) as e:
        print("{}: {}".format(MESSAGES['intersections_error'], e))


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)


def create_intersections_table():
    print(MESSAGES['intersections_create_info'])
    query = "SELECT process_intersections()"
    run_query(query)
    print(MESSAGES['intersections_create_success'])
