import psycopg2
import os


MESSAGES = {
    'intersections_create_info': '-- Creando datos de intersecciones',
    'intersections_create_success': 'Los datos de intersecciones fueron creados'
                                    ' exitosamente.'
}


def run():
    try:
        create_intersections_table()
    except Exception as e:
        print(e)


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query(query):
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
    except psycopg2.DatabaseError as e:
        print(e)


def create_intersections_table():
    print(MESSAGES['intersections_create_info'])
    query = "SELECT process_intersections()"
    run_query(query)
    print(MESSAGES['intersections_create_success'])
