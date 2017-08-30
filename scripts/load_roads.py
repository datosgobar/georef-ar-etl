from geo_admin.models import Road, Locality
import psycopg2
import json
import os


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


def run():
    roads = []
    failed_rows = []
    localities = {locality.code: (locality.id, locality.state_id)
                  for locality in Locality.objects.all()}
    try:
        streets = run_query()
        print('-- Procesando vías --')
        for row in streets:
            (code, name, road_type, start_left, start_right,
            end_left, end_right, geom, locality) = row
            if locality in localities:
                locality_id = localities[locality][0]
                state_id = localities[locality][1]
                roads.append(Road(
                    code=code,
                    name=name,
                    road_type=road_type,
                    start_left=start_left,
                    start_right=start_right,
                    end_left=end_left,
                    end_right=end_right,
                    geom=geom,
                    locality_id=locality_id,
                    state_id=state_id
                ))
            else:
                failed_rows.append({'nombre': name, 'codloc': locality, 'nomencla': code})
        print('-- Insertando vías --')
        Road.objects.bulk_create(roads)
        if failed_rows:
            print('-- Generando log de errores --')
            with open('failed_roads.json', 'w') as f:
                json.dump(failed_rows, f, indent=2)
        print('-- Proceso completo --')
    except Exception as e:
        print(e)
