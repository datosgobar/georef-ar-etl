from georef_ar_etl.exceptions import ProcessException
from georef_ar_etl.models import Department
from georef_ar_etl.departments import DepartmentsExtractionStep
from . import ETLTestCase

SAN_JUAN_DEPT_COUNT = 19


class TestDepartmentsExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_departments = self.create_test_departments()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Department).delete()
        self._ctx.session.query(self._tmp_departments).delete()
        super().tearDown()

    def test_single(self):
        """Los departamentos deberían poder ser procesados desde la tabla
        tmp_departamentos e insertados en la tabla correspondiente
        georef_departamentos."""
        step = DepartmentsExtractionStep()
        departments = step.run(self._tmp_departments, self._ctx)

        self.assertEqual(self._ctx.session.query(departments).count(),
                         SAN_JUAN_DEPT_COUNT)

        report_data = self._ctx.report.get_data('departments_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_DEPT_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de un departamento (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_departamentos."""
        # Ejecutar la extracción por primera vez
        step = DepartmentsExtractionStep()
        step.run(self._tmp_departments, self._ctx)

        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='70105').\
            update({'nam': 'Sarmientoo'})

        departments = step.run(self._tmp_departments, self._ctx)
        name = self._ctx.session.query(departments).\
            filter_by(id='70105').\
            one().nombre
        self.assertEqual(name, 'Sarmientoo')

    def test_id_change(self):
        """Si se modifica el ID de un departamento, se debería eliminar el
        departamento con el ID antiguo y se debería generar uno nuevo en la
        tabla georef_departamentos."""
        # Ejecutar la extracción por primera vez
        step = DepartmentsExtractionStep()
        step.run(self._tmp_departments, self._ctx)

        # Modificar el ID de un departamento
        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='70105').\
            update({'in1': '70100'})

        step.run(self._tmp_departments, self._ctx)
        report_data = self._ctx.report.get_data('departments_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['70100'])
        self.assertListEqual(report_data['deleted_entities_ids'], ['70105'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='70105').\
            update({'nam': '  Sarmiento\n\nSarmiento'})

        step = DepartmentsExtractionStep()
        departments = step.run(self._tmp_departments, self._ctx)
        name = self._ctx.session.query(departments).\
            filter_by(id='70105').\
            one().nombre
        self.assertEqual(name, 'Sarmiento')

    def test_id_length(self):
        """Si se recibe un departamento con longitud de ID inválida, no
        debería procesarse."""
        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='70105').\
            update({'in1': '701055'})

        step = DepartmentsExtractionStep()
        departments = step.run(self._tmp_departments, self._ctx)
        self.assertEqual(self._ctx.session.query(departments).count(),
                         SAN_JUAN_DEPT_COUNT - 1)

        report_data = self._ctx.report.get_data('departments_extraction')
        self.assertEqual(report_data['errors'][0][0], '701055')

    def test_invalid_province(self):
        """Si un departamento hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '79105'
        self._ctx.session.query(self._tmp_departments).\
            filter_by(in1='70105').\
            update({'in1': new_id})

        step = DepartmentsExtractionStep()
        departments = step.run(self._tmp_departments, self._ctx)
        query = self._ctx.session.query(departments).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('departments_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_DEPT_COUNT - 1)

    def test_invalid_commune(self):
        """Si un departamento de CABA tiene un ID no divisible por 7, debería
        generarse un error. Ver comentario en constants.py para más
        información."""
        self._ctx.session.add(self._tmp_departments(ogc_fid=9999, in1='02006'))
        step = DepartmentsExtractionStep()

        with self.assertRaises(ProcessException):
            step.run(self._tmp_departments, self._ctx)
