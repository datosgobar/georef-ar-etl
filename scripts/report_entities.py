import json
import psycopg2
import os
import datetime

entities = ['provincias', 'departamentos', 'municipios', 'bahra']
report = []


def run():
    try:
        for entity in entities:
            create_report_by_entity(entity)
        create_report()
    except Exception as e:
        print("{0}:", e)


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query_entities(query):
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()[0][0]
    except psycopg2.DatabaseError as e:
        print(e)


def create_report():
    output = [{
        'fecha_actualizacion': str(datetime.datetime.now()),
        'entidades': report
    }]
    filename = 'logs/entities_report.json'

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, 'w') as file:
        json.dump(output, file, indent=2, ensure_ascii=False)


def create_report_by_entity(entity):
    result = {entity: {
                'cantidades': get_count(entity),
                'entidades_nuevas': get_new_entities(entity),
                'codigos_actualizados': get_updates(entity, 'code'),
                'nombres_actualizados': get_updates(entity, 'name'),
                'geometrias_actualizadas': get_updates(entity, 'geom')
            }}
    report.append(result)


def get_new_entities(entity):
    column_code = 'in1'
    column_name = 'nam'

    if entity is 'bahra':
        column_code = 'cod_bahra'
        column_name = 'nombre_bah'

    query = "SELECT get_new_entities('{}', '{}', '{}')".\
        format(entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        new_entities = []
        for row in results:
            new_entities.append({
                'codigo': row['code'],
                'nombre': row['name']
            })
        return new_entities


def get_updates(entity, field):
    column_code = 'in1'
    column_name = 'nam'

    if entity is 'bahra':
        column_code = 'cod_bahra'
        column_name = 'nombre_bah'

    query = "SELECT get_entities_{}_updates('{}', '{}', '{}')".\
        format(field, entity, column_code, column_name)
    results = run_query_entities(query)
    if 'result' not in results:
        updates = []
        dict_updates = {}
        for row in results:
            dict_updates.update({
                'codigo': row['code'],
                'nombre': row['name']
            })
            if field is 'name' or field is 'code':
                dict_updates.update({'actualizacion': row['update']})
        updates.append(dict_updates)
        return updates


def get_count(entity):
    query = "SELECT get_quantities('{}')".format(entity)
    results = run_query_entities(query)
    return results
