from django.db import connection
from georef.settings import BASE_DIR


def run():
    file_path = BASE_DIR + '/etl_scripts/geocodificar.sql'
    with open(file_path, 'r') as f:
        geocode_function_definition = f.read()

    with connection.cursor() as cursor:
        cursor.execute('CREATE EXTENSION postgis;')
        cursor.execute(geocode_function_definition)
