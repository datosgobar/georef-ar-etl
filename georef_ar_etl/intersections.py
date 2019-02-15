from sqlalchemy.orm import aliased
from .etl import ETL
from .models import Province, Department, Street, Intersection
from . import constants, geometry


class IntersectionsETL(ETL):
    def __init__(self):
        super().__init__(constants.INTERSECTIONS, [Province, Street,
                                                   Department])

    def _run_internal(self, ctx):
        provinces = ctx.query(Province).all()
        total = len(provinces)

        ctx.query(Intersection).delete()

        for i, province in enumerate(provinces):
            self._insert_province_intersections(province, i + 1, total + 1,
                                                ctx)

        self._insert_province_intersections(None, total, total, ctx)

    def _build_intersection_query(self, province, bulk_size, ctx):
        StreetA = aliased(Street)
        StreetB = aliased(Street)
        query = ctx.query(StreetA, StreetB)

        if province:
            query = query.join(StreetB,
                               StreetA.provincia_id == StreetB.provincia_id)
        else:
            query = query.join(StreetB,
                               StreetA.provincia_id != StreetB.provincia_id)

        query = query.\
            filter(StreetA.id < StreetB.id).\
            filter(StreetA.geometria.ST_Intersects(StreetB.geometria))

        if province:
            query = query.\
                filter(StreetA.provincia_id == province.id).\
                filter(StreetB.provincia_id == province.id)

        return query.yield_per(bulk_size)

    def _insert_province_intersections(self, province, i, total, ctx):
        province_name = province.iso_nombre if province else 'interprovincial'
        ctx.logger.info('Creando intersecciones para: %s [%s/%s]',
                        province_name, i, total)
        ctx.logger.info('Procesando...')

        bulk_size = ctx.config.getint('etl', 'bulk_size')

        intersections = []
        count = 0
        query = self._build_intersection_query(province, bulk_size, ctx)

        for street_a, street_b in query:
            intersection = Intersection(
                id='{}-{}'.format(street_a.id, street_b.id),
                calle_a_id=street_a.id,
                calle_b_id=street_b.id,
                geometria=geometry.get_intersection_centroid(
                    street_a.geometria,
                    street_b.geometria,
                    ctx
                )
            )

            intersections.append(intersection)

            if len(intersections) > bulk_size:
                ctx.session.add_all(intersections)
                count += len(intersections)
                intersections.clear()

        ctx.session.add_all(intersections)
        count += len(intersections)
        ctx.logger.info('Intersecciones creadas, cantidad: %s.', count)
        ctx.logger.info('')
