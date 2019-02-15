from geoalchemy2.shape import to_shape


def get_centroid(entity, ctx, geom_field='geom'):
    # TODO: Hacer que reciba la geom directamente como en
    # get_intersection_centroid?
    centroid = ctx.session.scalar(
        getattr(entity, geom_field).ST_Centroid())
    point = to_shape(centroid)
    return point.x, point.y


def get_intersection_centroid(geom_a, geom_b, ctx):
    return ctx.session.scalar(geom_a.ST_Intersection(geom_b).ST_Centroid())


def get_intersection_percentage(entity_a, entity_b, ctx, geom_field_a='geom',
                                geom_field_b='geom'):
    # TODO: Hacer que reciba la geom directamente como en
    # get_intersection_centroid?
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


def get_entity_at_point(entity_type, point, ctx, geom_field='geometria'):
    return ctx.query(entity_type).filter(
        getattr(entity_type, geom_field).ST_Contains(point)
    ).one_or_none()
