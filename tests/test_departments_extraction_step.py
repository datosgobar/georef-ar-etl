from georef_ar_etl.provinces import ProvincesExtractionStep
from georef_ar_etl.departments import DepartmentsExtractionStep
from . import ETLTestCase


class TestDepartmentsExtractionStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self._tmp_provinces = self.create_test_provinces()
        self._tmp_departments = self.create_test_departments()

        step = ProvincesExtractionStep()
        step.run(self._tmp_provinces, self._ctx)

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(self._tmp_departments).delete()
        self._ctx.session.query(self._tmp_provinces).delete()
        super().tearDown()

    def test_single(self):
        """Los departamentos deberían poder ser procesados desde la tabla
        tmp_departamentos e insertados en la tabla correspondiente
        georef_departamentos."""
        step = DepartmentsExtractionStep()
        departments = step.run(self._tmp_departments, self._ctx)

        # Santa Fe tiene 19 departamentos
        self.assertEqual(self._ctx.session.query(departments).count(), 19)

        report_data = self._ctx.report.get_data('departments_extraction')
        self.assertEqual(len(report_data['new_entities_ids']), 19)

    def test_field_change(self):
        """Si se modifica un campo de un departamento (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_departamentos."""
        # Ejecutar la extracción por primera vez
        step = DepartmentsExtractionStep()
        step.run(self._tmp_departments, self._ctx)

        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='82077').\
            update({'nam': '8 de Julio'})

        departments = step.run(self._tmp_departments, self._ctx)
        name = self._ctx.session.query(departments).\
            filter_by(id='82077').\
            one().nombre
        self.assertEqual(name, '8 de Julio')
