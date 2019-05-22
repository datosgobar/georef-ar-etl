"""Módulo 'geometry' de georef-ar-etl.

Define funciones de utilidad para operar con geometrías.

"""

import json
import math
import binascii
from geoalchemy2.elements import WKBElement

# Radio de la tierra promedio para WGS84
_MEAN_EARTH_RADIUS_KM = 6371.0088


def get_centroid_coordinates(geom, ctx):
    centroid_json = ctx.session.scalar(geom.ST_Centroid().ST_AsGeoJSON())
    centroid = json.loads(centroid_json)
    return centroid['coordinates']


def distance_to_angle(distance_m):
    """Transforma una distancia en metros a una distancia en grados para
    utilizar junto al sistema de coordenadas WGS84.

    Args:
        distance_m (int): Distancia en metros.

    Returns:
        float: Ángulo equivalente a la distancia.

    """
    # Ángulo de un arco = longitud del arco / radio
    return math.degrees(distance_m / (1000 * _MEAN_EARTH_RADIUS_KM))


def get_streets_intersections(geom_a, geom_b, ctx, cluster_distance_m=50):
    """Dadas las geometrías de dos calles (MultiLineString), retorna todas las
    intersecciones de las mismas, utilizando un criterio razonable.

    Si las dos calles interseccionan en un único punto, se retorna ese punto.
    Si las dos calles interseccionan en más de un punto, se agrupan todos los
    puntos en clusters, donde cada cluster contiene puntos separados por no más
    de 'cluster_distance_m' metros. Por cada cluster, se calcula el centroide,
    y se retornan todos los centroides calculados. Esto es útil para calcular
    intersecciones de calles como avenidas con más de un carril, que
    técnicamente interseccionan con otras en varios puntos, pero se debería
    considerar una intersección única.

    Args:
        geom_a (geoalchemy2.Geometry): Geometría de la calle A.
        geom_b (geoalchemy2.Geometry): Geometría de la calle B.
        ctx (Context): Contexto de ejecución.
        cluster_distance_m (int): Distancia en metros por la cual agrupar
            puntos. El valor por defecto 50 fue elegido experimentalmente.

    Raises:
        ValueError: Si las calles no interseccionan.

    Returns:
        list: Lista de puntos de intersección. Cada punto es representado por
            un WKBElement de GeoAlchemy2.

    """
    points = [
        result[0] for result in
        ctx.session.query(geom_a.ST_Intersection(geom_b).ST_Dump().geom)
    ]

    if not points:
        raise ValueError('Geometries do not intersect.')
    if len(points) == 1:
        # La intersección de las geometrías es exactamente un punto, usar ese
        # mismo. La mayoría de los casos de intersecciones de calles caen acá.
        return points

    # OPTIMIZE: Optimizar casos de dos intersecciones?
    # En Misiones, el 98.71% de las intersecciones tienen un punto.
    # En CABA, el 97.87% de las intersecciones tienen un punto.
    # Parece que optimizar casos de dos intersecciones no traería una mejora
    # significativa.

    # Las geometrías interseccionan en más de un punto, realizar un
    # clusterizado de los puntos. Esto se hace para obtener las intersecciones
    # de calles que se cruzan en más de un punto, pero eliminando
    # intersecciones que estén demasiado cerca de otras. Esto sucede por
    # ejemplo cuando una calle con varios carriles intersecciona una calle
    # normal.

    # 1) Tomar el valor de cada punto y colocarlos en un string, para luego
    # crear un array SQL.
    values = ','.join([
        "ST_GeomFromEWKB('{}'::geometry)".format(val)
        for val in points
    ])

    # 2) Crear la sentencia SQL: se hace el ClusterWithin del array de puntos,
    # y luego por cada set de puntos del cluster se calcula el centroide. Esto
    # resulta en N puntos, donde cada punto representa un grupo de puntos
    # cercanos entre ellos.
    statement = """
        select ST_Centroid(unnest(ST_ClusterWithin(geom, {distance})))
        from unnest(array[{values}]) as geom
    """.format(values=values, distance=distance_to_angle(cluster_distance_m))

    return [
        WKBElement(binascii.unhexlify(row[0]), extended=True)
        for row in ctx.engine.execute(statement)
    ]


def get_intersection_percentage(geom_a, geom_b, ctx):
    area_a = ctx.session.scalar(geom_a.ST_Area())
    if area_a <= 0:
        raise ValueError('geom_a has area size 0.')

    area_isct = ctx.session.scalar(geom_a.ST_Intersection(geom_b).ST_Area())
    return area_isct / area_a


def get_entity_at_point(entity_type, point, ctx, geom_field='geometria'):
    # TODO: Sería mejor utilizar .one_or_none() en lugar de .first(), pero la
    # capa de municipios tiene dos municipios que ocupan el *mismo lugar
    # físico* (625035 y 620224). Cambiar cuando se resuelva ese problema.
    return ctx.session.query(entity_type).filter(
        getattr(entity_type, geom_field).ST_Contains(point)
    ).first()
