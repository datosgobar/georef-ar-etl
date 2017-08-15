from geo_admin.models import Road, Locality
import psycopg2


def get_db_connection():
    return psycopg2.connect(
        dbname='indec_geodata',
        user='postgres',
        password='postgres')


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
                    FROM  indec_vias; 
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
    localities = {locality.code: (locality.id, locality.state_id)
                  for locality in Locality.objects.all()}
    try:
        streets = run_query()
        for row in streets:
            code, name, road_type, start_left, start_right, end_left, \
            end_right, geom, codloc = row
            if codloc in localities:
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
        Road.objects.bulk_create(roads)
    except Exception as e:
        print(e)
