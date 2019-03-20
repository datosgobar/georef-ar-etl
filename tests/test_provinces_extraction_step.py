from georef_ar_etl.provinces import ProvincesExtractionStep
from . import ETLTestCase


class TestProvincesExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._raw_provinces = cls.create_test_provinces()

    def tearDown(self):
        self._ctx.session.query(self._raw_provinces).delete()
        super().tearDown()

    def test_single(self):
        """Una sola provincia debería poder ser procesada desde la tabla
        raw_provincias e insertada en la tabla correspondiente
        georef_provincias."""
        step = ProvincesExtractionStep()
        provinces = step.run(self._raw_provinces, self._ctx)

        self.assertEqual(self._ctx.session.query(provinces).count(), 1)
        self.assertEqual(
            self._ctx.session.query(provinces).first().nombre,
            self._ctx.session.query(self._raw_provinces).first().nam)
