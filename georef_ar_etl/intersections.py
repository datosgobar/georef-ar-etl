from sqlalchemy.orm import aliased
from .loaders import CompositeStepCreateFile, CompositeStepCopyFile
from .process import Process, Step, CompositeStep
from .models import Province, Department, Street, Intersection
from .exceptions import ProcessException
from . import constants, geometry, utils, loaders

MAX_POINTS_PER_INTERSECTION = 99


def create_process(config):
    output_path = config.get('etl', 'output_dest_path')

    return Process(constants.INTERSECTIONS, [
        utils.CheckDependenciesStep([Province, Department, Street]),
        IntersectionsCreationStep(),
        utils.ValidateTableSizeStep(
            target_size=config.getint('etl', 'intersections_target_size'),
            op='ge'),
        CompositeStepCreateFile(
            Intersection, 'intersections', config,
            tolerance=config.getfloat("etl", "geojson_tolerance"),
            caba_tolerance=config.getfloat("etl", "geojson_caba_tolerance")
        ),
        CompositeStepCopyFile('intersections', config),
    ])


class IntersectionsCreationStep(Step):
    def __init__(self):
        super().__init__('intersections_creation_step', reads_input=False)

    def _run_internal(self, data, ctx):
        provinces = ctx.session.query(Province).all()
        total = len(provinces)

        ctx.session.query(Intersection).delete()

        for i, province in enumerate(provinces):
            self._insert_province_intersections(province, i + 1, total + 1,
                                                ctx)
        self._insert_province_intersections(None, total + 1, total + 1, ctx)

        return Intersection

    def _build_intersection_query(self, province, bulk_size, ctx):
        StreetA = aliased(Street)
        StreetB = aliased(Street)
        query = ctx.session.query(StreetA, StreetB)

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
        ctx.report.info('Creando intersecciones para: %s [%s/%s]',
                        province_name, i, total)
        ctx.report.info('Procesando...')

        bulk_size = ctx.config.getint('etl', 'bulk_size')

        count = 0
        query = self._build_intersection_query(province, bulk_size, ctx)

        for street_a, street_b in utils.pbar(query, ctx):
            points = geometry.get_streets_intersections(street_a.geometria,
                                                        street_b.geometria,
                                                        ctx)

            for idx, point in enumerate(points):
                if idx + 1 > MAX_POINTS_PER_INTERSECTION:
                    raise ProcessException('Más de {} puntos para intersección'
                                           ' de calles con IDs {} y {}'.format(
                                               MAX_POINTS_PER_INTERSECTION,
                                               street_a.id, street_b.id
                                           ))

                isct_num = str(idx + 1).rjust(2, '0')
                intersection = Intersection(
                    id='{}-{}-{}'.format(street_a.id, street_b.id, isct_num),
                    calle_a_id=street_a.id,
                    calle_b_id=street_b.id,
                    geometria=point
                )

                utils.add_maybe_flush(intersection, ctx, bulk_size)
                count += 1

        ctx.report.info('Intersecciones creadas, cantidad: %s.\n', count)
