from georef_ar_etl.models import Intersection, Street
from georef_ar_etl.intersections import IntersectionsCreationStep
from . import ETLTestCase

SAN_JUAN_INTERSECTIONS_COUNT = 1578


class TestIntersectionsCreationStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_census_localities(extract=True)
        cls._tmp_blocks = cls.create_test_blocks()
        cls.extract_streets(cls._tmp_blocks)

        # Las intersecciones no se modifican entre tests
        step = IntersectionsCreationStep()
        step.run(None, cls._ctx)

    def test_intersections_creation(self):
        """Se deberían calcular las intersecciones de calles para cada
        provincia."""
        self.assertEqual(self._ctx.session.query(Intersection).count(),
                         SAN_JUAN_INTERSECTIONS_COUNT)

    def test_intersections_duplicates(self):
        """Por cada par de calle (X, Y) que se crucen, se debería crear la
        intersección (X, Y) o (Y, X), pero no ambas."""
        reversed_ids = (
            '-'.join(reversed(isct.id.split('-')))
            for isct in self._ctx.session.query(Intersection)
        )

        self.assertEqual(self._ctx.session.query(Intersection).filter(
            Intersection.id in reversed_ids).count(), 0)

    def test_multiple_intersections(self):
        """Algunas intersecciones de calles deberían estar representadas por
        varios puntos, con el ID de cada uno siendo una secuencia 1, 2, etc."""

        # Dos intersecciones de "ABERASTAIN" y "PJE S N" separadas por
        # aproximadamente 85 metros.
        self.assertTrue(self._ctx.session.query(Intersection).get(
            '7003501000045-7003501001120-2'))
        self.assertTrue(self._ctx.session.query(Intersection).get(
            '7003501000045-7003501001120-1'))

        # Intersección de "CALLE S N" y "CALLE S N" (un solo punto)
        self.assertTrue(self._ctx.session.query(Intersection).get(
            '7003501000145-7003501000265-1'))
        self.assertFalse(self._ctx.session.query(Intersection).get(
            '7003501000145-7003501000265-2'))

    def test_self_intersection(self):
        """Las calles no deberían intersectar con sí mismas."""
        ids = (
            '-'.join([street.id] * 2)
            for street in self._ctx.session.query(Street)
        )

        self.assertEqual(self._ctx.session.query(Intersection).filter(
            Intersection.id in ids).count(), 0)
