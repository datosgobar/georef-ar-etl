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
    try:
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
        with get_db_connection().cursor() as cursor:
            cursor.execute(query)
            streets = cursor.fetchall()
            cursor.close()
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
            code, name, road_type, start_left, start_right, end_left, \
            end_right, geom, codloc = row
            if codloc in localities and localities[codloc][1]:
                # codloc = '02000010' if '02000000' < codloc < '03000000' else codloc
                locality_id = localities[codloc][0]
                state_id = localities[codloc][1]
                roads.append(Road(
                    code=code,
                    name=name,
                    road_type=road_type,
                    start_left=start_left,
                    start_right=start_right,
                    end_left=end_left,
                    end_right=end_right,
                    geom=geom,
                    locality_id=locality_id if locality_id else None,
                    state_id=state_id if state_id else None,
                ))
            else:
                failed_rows.append({'nombre': name, 'codloc': codloc, 'nomencla': code})
        print('-- Insertando vías --')
        Road.objects.bulk_create(roads)
        if failed_rows:
            print('-- Generando log de errores --')
            with open('failed_roads.json', 'w') as f:
                json.dump(failed_rows, f, indent=4)
        print('-- Proceso completo --')
    except Exception as e:
        print(e)
