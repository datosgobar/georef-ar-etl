from geoalchemy2.shape import to_shape


def get_centroid(entity, ctx, geom_field='geom'):
    centroid = ctx.session.scalar(
        getattr(entity, geom_field).ST_Centroid())
    point = to_shape(centroid)
    return point.x, point.y
