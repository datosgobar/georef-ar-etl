from geo_admin.models import Road, Locality
from datetime import datetime
import psycopg2
import json
import os


roads = []
failed_roads = []
localities = {locality.code: (locality.id, locality.state_id)
                for locality in Locality.objects.all()}


def run():
    print('-- Procesando vías --')
    try:
        streets = run_query()
        for row in streets:
            process_street(row)
        print('-- Insertando vías --')
        Road.objects.bulk_create(roads)
        print('-- Proceso completo --')
        generate_report()
    except Exception as e:
        print(e)


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'),
        dbname=os.environ.get('POSTGRES_DBNAME'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'))


def run_query():
    query = """SELECT nomencla as code, \
                    nombre as name, \
                    tipo as road_type, \
                    desdei as start_left, \
                    desded as start_right, \
                    hastai as end_left, \
                    hastad as end_right, \
                    geom, \
                    codloc as locality \
            FROM  indec_vias \
            WHERE tipo <> 'OTRO';
        """
    try:
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            streets = cursor.fetchall()
        return streets
    except psycopg2.DatabaseError as e:
        print(e)


def generate_report():
    timestamp = datetime.now().strftime('%d-%m-%Y a las %H:%M:%S')
    heading = 'Proceso de ETL de datos INDEC ejecutado el %s.\n' % timestamp
    ok_roads_msg = '-- Calles procesadas exitosamente: %s' % len(roads)
    failed_roads_msg = '-- Calles con errores: %s' % len(failed_roads)

    with open('report.txt', 'a') as report:
        print('-- Generando reporte --')
        report.write(heading)
        report.write(ok_roads_msg + '\n')
        report.write(failed_roads_msg + '\n\n')

    if failed_roads:
        print('-- Generando log de errores --')
        with open('failed_roads.json', 'w') as report:
            json.dump(failed_roads, report, indent=2)

    print('** Resultado del proceso **')
    print(ok_roads_msg)
    print(failed_roads_msg)


def process_street(row):
    (code, name, road_type, start_left, start_right,
    end_left, end_right, geom, locality) = row
    if locality in localities:
        locality_id = localities[locality][0]
        state_id = localities[locality][1]
        road = Road(code=code, name=name, road_type=road_type,
                    start_left=start_left, start_right=start_right,
                    end_left=end_left, end_right=end_right, geom=geom,
                    locality_id=locality_id, state_id=state_id)
        roads.append(road)
    else:
        failed_roads.append({'nombre': name, 'codloc': locality, 'nomencla': code})