import json
import psycopg2
import os


MESSAGES = {
    'intersections_data_info': '-- Creando datos de intersecciones.',
    'intersections_success': 'Los datos de intersecciones para la Provincia '
                             'con código "%s" fueron creados exitosamente.',
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
            entities = cursor.fetchall()
        return entities
    except psycopg2.DatabaseError as e:
        print(e)


def create_intersections_table():
    query = """SELECT create_intersections(in1)
               FROM ign_provincias
               ORDER BY in1 ASC ;
            """
    intersections = run_query(query)
    return intersections


def create_intersections_data():
    print(MESSAGES['intersections_data_info'])
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

        print(MESSAGES['intersections_success'] % code)
