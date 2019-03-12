import json


def get_centroid_coordinates(geom, ctx):
    centroid_json = ctx.session.scalar(geom.ST_Centroid().ST_AsGeoJSON())
    centroid = json.loads(centroid_json)
    return centroid['coordinates']


def get_intersection_centroid(geom_a, geom_b, ctx):
    return ctx.session.scalar(geom_a.ST_Intersection(geom_b).ST_Centroid())


def get_intersection_percentage(geom_a, geom_b, ctx):
    if ctx.mode == 'interactive':
        # Saltear operaciones costosas en modo interactivo
        return 0

    area_a = ctx.session.scalar(geom_a.ST_Area())

    if area_a <= 0:
        raise ValueError('geom_a has area size 0')

    area_isct = ctx.session.scalar(geom_a.ST_Intersection(geom_b).ST_Area())
    return area_isct / area_a


def get_entity_at_point(entity_type, point, ctx, geom_field='geometria'):
    return ctx.session.query(entity_type).filter(
        getattr(entity_type, geom_field).ST_Contains(point)
    ).one_or_none()
