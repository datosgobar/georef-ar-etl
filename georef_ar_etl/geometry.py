from geoalchemy2.shape import to_shape


def get_centroid(entity, ctx, geom_field='geom'):
    centroid = ctx.session.scalar(
        getattr(entity, geom_field).ST_Centroid())
    point = to_shape(centroid)
    return point.x, point.y


def get_intersection_percentage(entity_a, entity_b, ctx, geom_field_a='geom',
                                geom_field_b='geom'):
    if ctx.mode == 'interactive':
        # Saltear operaciones costosas en modo interactivo
        return 0

    geom_a = getattr(entity_a, geom_field_a)
    geom_b = getattr(entity_b, geom_field_b)
    area_a = ctx.session.scalar(geom_a.ST_Area())

    if area_a <= 0:
        raise ValueError('Department has area size 0')

    area_isct = ctx.session.scalar(geom_a.ST_Intersection(geom_b).ST_Area())
    return area_isct / area_a
