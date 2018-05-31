# -*- coding: utf-8 -*-

import json
import psycopg2
import os


MESSAGES = {
    'intersections_export_info': '-- Exportando datos de intersecciones.',
    'intersections_export_success': 'Los datos de intersecciones para la '
                                    'Provincia con código "%s" fueron '
                                    'exportados exitosamente.',
}


def run():
    try:
        create_intersections_data()
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
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except psycopg2.DatabaseError as e:
        print(e)


def create_intersections_data():
    print(MESSAGES['intersections_export_info'])
    entities_code = ['02', '06', '10', '14', '18', '22', '26', '30', '34', '38',
                     '42', '46', '50', '54', '58', '62', '66', '70', '74', '78',
                     '82', '86', '90', '94']

    query = """SELECT 
                nomencla_a,
                nombre_a, 
                tipo_a,
                departamento_a,
                provincia_a, 
                nomencla_b, 
                nombre_b,
                tipo_b,
                departamento_b,
                provincia_b,
                ST_Y(geom) AS lat, 
                ST_X(geom) AS lon
               FROM {}
            """

    for code in entities_code:
        data = []
        table_name = 'indec_intersecciones_{}'.format(code)
        intersections = run_query(query.format(table_name))
        for row in intersections:
            (nomencla_a, nombre_a, tipo_a, departamento_a, provincia_a,
             nomencla_b, nombre_b, tipo_b, departamento_b, provincia_b,
             lat, lon) = row
            data.append({
                'nomencla_a': nomencla_a,
                'nombre_a': nombre_a,
                'tipo_a': tipo_a,
                'departamento_a': departamento_a,
                'provincia_a': provincia_a,
                'nomencla_b': nomencla_b,
                'nombre_b': nombre_b,
                'tipo_b': tipo_b,
                'departamento_b': departamento_b,
                'provincia_b': provincia_b,
                'lat': lat,
                'lon': lon
            })
        filename = 'data/intersecciones/indec_intersecciones_{}.json'\
            .format(code)

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename, 'w') as outfile:
            json.dump(data, outfile, ensure_ascii=False)

        print(MESSAGES['intersections_export_success'] % code)
