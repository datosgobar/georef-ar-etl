import json
import psycopg2


def get_db_connection():
    return psycopg2.connect(
        dbname='georef_test',
        user='postgres',
        password='postgres')


def update_db_table(roads):
	cmd = """INSERT INTO public.calles \
				(tipo, nombre, localidad, \
				inicio_derecha, inicio_izquierda, \
				fin_derecha, fin_izquierda) \
			VALUES ('%(tipo)s', '%(nombre)s', '%(localidad)s', \
					%(inicio_derecha)s, %(inicio_izquierda)s, \
					%(fin_derecha)s, %(fin_izquierda)s);"""
	connection = get_db_connection()
	with connection.cursor() as cursor:
		print('-- Agregando registros a la base de datos...')
		for road in roads:
			#print(cmd % road)
			cursor.execute(cmd % road)
		connection.commit()
		print('-- Operación finalizada.')


def get_road_from(feature):
	return {
		'nombre': feature['properties']['nombre'],
		'tipo': feature['properties']['tipo'],
		'localidad': feature['properties']['codloc'],
		'inicio_derecha': feature['properties']['desded'] or 0,
		'inicio_izquierda': feature['properties']['desdei'] or 0,
		'fin_derecha': feature['properties']['hastad'] or 0,
		'fin_izquierda': feature['properties']['hastai'] or 0,
		'geometria': {
			'coordenadas': feature['geometry']['coordinates'],
			'tipo': feature['geometry']['type']
		}
	}


def main():
	with open('../georef-data/vias_geoserver.json') as roads_file:
		data = json.load(roads_file)
	roads = []
	for feature in data.get('features', []):
		road = get_road_from(feature)
		roads.append(road)
	update_db_table(roads)


if __name__ == '__main__':
    main()