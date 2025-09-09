from georef_ar_etl.models import LocalGovernment
from georef_ar_etl.exceptions import ValidationException
from georef_ar_etl.local_governments import LocalGovernmentsExtractionStep
from tests import ETLTestCase

SAN_JUAN_MUNI_COUNT = 19


class TestLocalGovernmentsExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_local_governments = self.create_test_local_governments()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(LocalGovernment).delete()
        self._ctx.session.query(self._tmp_local_governments).delete()
        super().tearDown()

    def test_single(self):
        """Los municipios deberían poder ser procesados desde la tabla
        tmp_municipios e insertados en la tabla correspondiente
        georef_municipios."""
        step = LocalGovernmentsExtractionStep()
        municipalities = step.run(self._tmp_local_governments, self._ctx)

        self.assertEqual(self._ctx.session.query(municipalities).count(),
                         SAN_JUAN_MUNI_COUNT)

        report_data = self._ctx.report.get_data('local_governments_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_MUNI_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de un municipio (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_municipios."""
        # Ejecutar la extracción por primera vez
        lg_id = '700133'
        step = LocalGovernmentsExtractionStep()
        step.run(self._tmp_local_governments, self._ctx)

        self._ctx.session.query(self._tmp_local_governments).\
            filter_by(cmu=lg_id).\
            update({'nam': 'Zonda Zonda'})

        local_governments = step.run(self._tmp_local_governments, self._ctx)
        name = self._ctx.session.query(local_governments).\
            filter_by(id=lg_id).\
            one().nombre
        self.assertEqual(name, 'Zonda Zonda')

    def test_id_change(self):
        """Si se modifica el ID de un gobierno local, se debería eliminar el
        gobierno local con el ID antiguo y se debería generar uno nuevo en la
        tabla georef_gobiernos_locales."""
        # Ejecutar la extracción por primera vez
        step = LocalGovernmentsExtractionStep()
        step.run(self._tmp_local_governments, self._ctx)

        # Modificar el ID de un municipio
        self._ctx.session.query(self._tmp_local_governments).\
            filter_by(cmu='700133').\
            update({'cmu': '700500'})

        step.run(self._tmp_local_governments, self._ctx)
        report_data = self._ctx.report.get_data('local_governments_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['700500'])
        self.assertListEqual(report_data['deleted_entities_ids'], ['700133'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        self._ctx.session.query(self._tmp_local_governments).\
            filter_by(cmu='700133').\
            update({'nam': '  Zonda   \n\n'})

        step = LocalGovernmentsExtractionStep()
        municipalities = step.run(self._tmp_local_governments, self._ctx)
        name = self._ctx.session.query(municipalities).\
            filter_by(id='700133').\
            one().nombre
        self.assertEqual(name, 'Zonda')

    def test_id_length(self):
        """No se debería poder crear un municipio con longitud de ID
        inválida."""
        step = LocalGovernmentsExtractionStep()
        municipality = self._ctx.session.query(self._tmp_local_governments).\
            filter_by(cmu='700133').one()

        self._ctx.session.expunge(municipality)
        municipality.cmu = '7001333'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(municipality, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si un municipio hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '790133'
        self._ctx.session.query(self._tmp_local_governments).\
            filter_by(cmu='700133').\
            update({'cmu': new_id})

        step = LocalGovernmentsExtractionStep()
        municipalities = step.run(self._tmp_local_governments, self._ctx)
        query = self._ctx.session.query(municipalities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('local_governments_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_MUNI_COUNT - 1)
