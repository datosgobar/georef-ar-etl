from django.db import connection
from georef.settings import BASE_DIR
import psycopg2


def run():
    try:
        files_path = [
            BASE_DIR + '/etl_scripts/function_geocodificar.sql',
            BASE_DIR + '/etl_scripts/function_get_department.sql'
        ]

        for file in files_path:
            with open(file, 'r') as f:
                func = f.read()
            with connection.cursor() as cursor:
                cursor.execute(func)
        print('-- Se cargaron las funciones exitosamente.')
    except psycopg2.DatabaseError as e:
        print(e)

