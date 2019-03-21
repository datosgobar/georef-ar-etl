from georef_ar_etl.provinces import ProvincesExtractionStep
from . import ETLTestCase


class TestProvincesExtractionStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self._tmp_provinces = self.create_test_provinces()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(self._tmp_provinces).delete()
        super().tearDown()

    def test_single(self):
        """Una sola provincia debería poder ser procesada desde la tabla
        tmp_provincias e insertada en la tabla correspondiente
        georef_provincias."""
        step = ProvincesExtractionStep()
        provinces = step.run(self._tmp_provinces, self._ctx)

        self.assertEqual(self._ctx.session.query(provinces).count(), 1)
        self.assertEqual(
            self._ctx.session.query(provinces).first().nombre,
            self._ctx.session.query(self._tmp_provinces).first().nam)

        report_data = self._ctx.report.get_data('provinces_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['82'])

    def test_field_change(self):
        """Si se modifica un campo de una provincia (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_provincias."""
        # Ejecutar la extracción por primera vez
        step = ProvincesExtractionStep()
        step.run(self._tmp_provinces, self._ctx)

        self._ctx.session.query(self._tmp_provinces).\
            filter_by(in1='82').\
            update({'nam': 'Sta Fe'})

        provinces = step.run(self._tmp_provinces, self._ctx)
        self.assertEqual(self._ctx.session.query(provinces).first().nombre,
                         'Sta Fe')

        report_data = self._ctx.report.get_data('provinces_extraction')
        self.assertListEqual(report_data['new_entities_ids'], [])
        self.assertListEqual(report_data['deleted_entities_ids'], [])
