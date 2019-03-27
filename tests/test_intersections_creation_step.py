from georef_ar_etl.models import Intersection, Street
from georef_ar_etl.intersections import IntersectionsCreationStep
from . import ETLTestCase

SAN_JUAN_INTERSECTIONS_COUNT = 1560


class TestIntersectionsCreationStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_streets(extract=True)

        # Las intersecciones no se modifican entre tests
        step = IntersectionsCreationStep()
        step.run(None, cls._ctx)

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Intersection).delete()
        super().tearDown()

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

    def test_self_intersection(self):
        """Las calles no deberían intersectar con sí mismas."""
        ids = (
            '-'.join([street.id] * 2)
            for street in self._ctx.session.query(Street)
        )

        self.assertEqual(self._ctx.session.query(Intersection).filter(
            Intersection.id in ids).count(), 0)
