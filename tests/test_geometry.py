from sqlalchemy.sql import sqltypes
from geoalchemy2.types import Geometry
from georef_ar_etl import geometry
from . import ETLTestCase

TEST_MULTIPOLYGON = 'SRID=4326;MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)))'
TEST_MULTIPOLYGON_B = 'SRID=4326;MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))'
TEST_MULTILINESTRING = 'SRID=4326;MULTILINESTRING((0 0, 10 10))'
TEST_MULTILINESTRING_B = 'SRID=4326;MULTILINESTRING((10 0, 0 10))'
TEST_MULTILINESTRING_C = ('SRID=4326;MULTILINESTRING((0 0, 10 10),'
                          '(0.001 0, 10.001 10))')
TEST_MULTILINESTRING_D = 'SRID=4326;MULTILINESTRING((0 0, 20 20))'
TEST_POINT = 'SRID=4326;POINT(9 9)'


class TestGeometry(ETLTestCase):
    def test_get_centroid_coordinates(self):
        """La función get_centroid_coordinates debería retornar el centroide de
        una geometría."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTIPOLYGON')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTIPOLYGON)
        self._ctx.session.add(entity)
        self._ctx.session.commit()
        centroid = geometry.get_centroid_coordinates(entity.geom, self._ctx)

        self.assertAlmostEqual(centroid[0], 5)
        self.assertAlmostEqual(centroid[1], 5)

    def test_streets_intersections_single(self):
        """La función get_streets_intersections debería retornar una sola
        intersección si las dos calles solo interseccionan en un punto."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTILINESTRING')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTILINESTRING)
        entity_b = tbl(id=1, geom=TEST_MULTILINESTRING_B)
        self._ctx.session.add_all([entity, entity_b])
        self._ctx.session.commit()

        points = geometry.get_streets_intersections(entity.geom, entity_b.geom,
                                                    self._ctx)

        self.assertEqual(len(points), 1)
        centroid = geometry.get_centroid_coordinates(points[0], self._ctx)
        self.assertAlmostEqual(centroid[0], 5)
        self.assertAlmostEqual(centroid[1], 5)

    def test_streets_intersections_multiple(self):
        """La función get_streets_intersections debería retornar varias
        intersecciones si las intersecciones de las calles están
        suficientemente alejadas."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTILINESTRING')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTILINESTRING_B)
        entity_b = tbl(id=1, geom=TEST_MULTILINESTRING_C)
        self._ctx.session.add_all([entity, entity_b])
        self._ctx.session.commit()

        points = geometry.get_streets_intersections(entity.geom, entity_b.geom,
                                                    self._ctx)
        self.assertEqual(len(points), 2)

    def test_streets_intersections_multiple_merged(self):
        """Si el rango de tolerancia de get_streets_intersections es
        suficientemente alto, entonces varios puntos de intersección deberían
        combinarse en uno solo."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTILINESTRING')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTILINESTRING_B)
        entity_b = tbl(id=1, geom=TEST_MULTILINESTRING_C)
        self._ctx.session.add_all([entity, entity_b])
        self._ctx.session.commit()

        points = geometry.get_streets_intersections(entity.geom, entity_b.geom,
                                                    self._ctx,
                                                    cluster_distance_m=100)
        self.assertEqual(len(points), 1)

    def test_streets_intersections_overlap(self):
        """Si dos calles se solapan entre sí (puede suceder con los datos
        actuales de INDEC), se debería calcular la intersección
        correctamente."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTILINESTRING')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTILINESTRING)
        entity_b = tbl(id=1, geom=TEST_MULTILINESTRING_D)
        self._ctx.session.add_all([entity, entity_b])
        self._ctx.session.commit()

        points = geometry.get_streets_intersections(entity.geom, entity_b.geom,
                                                    self._ctx)
        self.assertEqual(len(points), 2)

    def test_get_intersection_percentage(self):
        """La función get_intersection_percentage debería devolver el
        porcentaje de área que ocupa una geometría sobre otra."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTIPOLYGON')
        }, pkey='id')

        entity = tbl(id=0, geom=TEST_MULTIPOLYGON)
        entity_b = tbl(id=1, geom=TEST_MULTIPOLYGON_B)
        self._ctx.session.add_all([entity, entity_b])
        self._ctx.session.commit()

        pctg = geometry.get_intersection_percentage(entity.geom, entity_b.geom,
                                                    self._ctx)
        self.assertAlmostEqual(pctg, 0.25)

    def test_get_entity_at_point(self):
        """La función get_entity_at_point devería devolver la entidad que
        contiene el punto especificado."""
        tbl = self.create_table('tbl', {
            'id': sqltypes.Integer,
            'geom': Geometry('MULTIPOLYGON')
        }, pkey='id')

        entity_id = 99
        entity = tbl(id=entity_id, geom=TEST_MULTIPOLYGON)
        self._ctx.session.add(entity)
        self._ctx.session.commit()
        found = geometry.get_entity_at_point(tbl, TEST_POINT, self._ctx,
                                             geom_field='geom')

        self.assertEqual(found.id, entity_id)
