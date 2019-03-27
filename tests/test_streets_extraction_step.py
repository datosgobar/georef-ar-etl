from georef_ar_etl.models import Street
from georef_ar_etl.exceptions import ValidationException, ProcessException
from georef_ar_etl.streets import StreetsExtractionStep
from . import ETLTestCase

SAN_JUAN_STREETS_COUNT = 754


class TestStreetsExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_streets = self.create_test_streets()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Street).delete()
        self._ctx.session.query(self._tmp_streets).delete()
        super().tearDown()

    def test_single(self):
        """Las calles deberían poder ser procesadas desde la tabla
        tmp_calles e insertadas en la tabla correspondiente
        georef_calles."""
        step = StreetsExtractionStep()
        streets = step.run(self._tmp_streets, self._ctx)

        self.assertEqual(self._ctx.session.query(streets).count(),
                         SAN_JUAN_STREETS_COUNT)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_STREETS_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de una calle (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_calles."""
        # Ejecutar la extracción por primera vez
        street_id = '7003501000900'
        step = StreetsExtractionStep()
        step.run(self._tmp_streets, self._ctx)

        self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla=street_id).\
            update({'nombre': 'LAPRIDA2'})

        streets = step.run(self._tmp_streets, self._ctx)
        name = self._ctx.session.query(streets).\
            filter_by(id=street_id).\
            one().nombre
        self.assertEqual(name, 'LAPRIDA2')

    def test_id_change(self):
        """Si se modifica el ID de una calle, se debería eliminar la
        calle con el ID antiguo y se debería generar una nueva en la
        tabla georef_calles."""
        # Ejecutar la extracción por primera vez
        step = StreetsExtractionStep()
        step.run(self._tmp_streets, self._ctx)

        # Modificar el ID de un calle
        self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla='7003501000900').\
            update({'nomencla': '7003501000999'})

        step.run(self._tmp_streets, self._ctx)
        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertListEqual(report_data['new_entities_ids'],
                             ['7003501000999'])
        self.assertListEqual(report_data['deleted_entities_ids'],
                             ['7003501000900'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        street_id = '7003501000900'
        self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla=street_id).\
            update({'nombre': '  LAPRIDA   \n\nLAPRIDA\n'})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_streets, self._ctx)
        name = self._ctx.session.query(streets).\
            filter_by(id=street_id).\
            one().nombre
        self.assertEqual(name, 'LAPRIDA')

    def test_id_length(self):
        """No se debería poder crear una calle con longitud de ID
        inválida."""
        step = StreetsExtractionStep()
        street = self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla='7003501000900').one()

        self._ctx.session.expunge(street)
        street.nomencla = '70035010009'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(street, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si una calle hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '7703501000900'
        self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla='7003501000900').\
            update({'nomencla': new_id})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_streets, self._ctx)
        query = self._ctx.session.query(streets).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_STREETS_COUNT - 1)

    def test_invalid_department(self):
        """Si una calle hace referencia a un departamento inexistente, se
        debería reportar el error."""
        new_id = '7011101000900'
        self._ctx.session.query(self._tmp_streets).\
            filter_by(nomencla='7003501000900').\
            update({'nomencla': new_id})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_streets, self._ctx)
        query = self._ctx.session.query(streets).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_STREETS_COUNT - 1)

    def test_invalid_commune(self):
        """Si una calle de CABA tiene un ID de departamento no divisible por 7,
        debería generarse un error. Ver comentario en constants.py para más
        información."""
        self._ctx.session.add(self._tmp_streets(ogc_fid=9999,
                                                nomencla='0200200000000'))
        step = StreetsExtractionStep()

        with self.assertRaises(ProcessException):
            step.run(self._tmp_streets, self._ctx)
