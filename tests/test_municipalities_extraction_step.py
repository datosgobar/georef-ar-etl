from georef_ar_etl.models import Municipality
from georef_ar_etl.exceptions import ValidationException
from georef_ar_etl.municipalities import MunicipalitiesExtractionStep
from . import ETLTestCase

SAN_JUAN_MUNI_COUNT = 19


class TestMunicipalitiesExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_municipalities = self.create_test_municipalities()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Municipality).delete()
        self._ctx.session.query(self._tmp_municipalities).delete()
        super().tearDown()

    def test_single(self):
        """Los municipios deberían poder ser procesados desde la tabla
        tmp_municipios e insertados en la tabla correspondiente
        georef_municipios."""
        step = MunicipalitiesExtractionStep()
        municipalities = step.run(self._tmp_municipalities, self._ctx)

        self.assertEqual(self._ctx.session.query(municipalities).count(),
                         SAN_JUAN_MUNI_COUNT)

        report_data = self._ctx.report.get_data('municipalities_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_MUNI_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de un municipio (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_municipios."""
        # Ejecutar la extracción por primera vez
        muni_id = '700133'
        step = MunicipalitiesExtractionStep()
        step.run(self._tmp_municipalities, self._ctx)

        self._ctx.session.query(self._tmp_municipalities).\
            filter_by(in1=muni_id).\
            update({'nam': 'Zonda Zonda'})

        municipalities = step.run(self._tmp_municipalities, self._ctx)
        name = self._ctx.session.query(municipalities).\
            filter_by(id=muni_id).\
            one().nombre
        self.assertEqual(name, 'Zonda Zonda')

    def test_id_change(self):
        """Si se modifica el ID de un municipio, se debería eliminar el
        municipio con el ID antiguo y se debería generar uno nuevo en la
        tabla georef_municipios."""
        # Ejecutar la extracción por primera vez
        step = MunicipalitiesExtractionStep()
        step.run(self._tmp_municipalities, self._ctx)

        # Modificar el ID de un municipio
        self._ctx.session.query(self._tmp_municipalities).\
            filter_by(in1='700133').\
            update({'in1': '700500'})

        step.run(self._tmp_municipalities, self._ctx)
        report_data = self._ctx.report.get_data('municipalities_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['700500'])
        self.assertListEqual(report_data['deleted_entities_ids'], ['700133'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        self._ctx.session.query(self._tmp_municipalities).\
            filter_by(in1='700133').\
            update({'nam': '  Zonda   \n\n'})

        step = MunicipalitiesExtractionStep()
        municipalities = step.run(self._tmp_municipalities, self._ctx)
        name = self._ctx.session.query(municipalities).\
            filter_by(id='700133').\
            one().nombre
        self.assertEqual(name, 'Zonda')

    def test_id_length(self):
        """No se debería poder crear un municipio con longitud de ID
        inválida."""
        step = MunicipalitiesExtractionStep()
        municipality = self._ctx.session.query(self._tmp_municipalities).\
            filter_by(in1='700133').one()

        self._ctx.session.expunge(municipality)
        municipality.in1 = '7001333'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(municipality, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si un municipio hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '790133'
        self._ctx.session.query(self._tmp_municipalities).\
            filter_by(in1='700133').\
            update({'in1': new_id})

        step = MunicipalitiesExtractionStep()
        municipalities = step.run(self._tmp_municipalities, self._ctx)
        query = self._ctx.session.query(municipalities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('municipalities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_MUNI_COUNT - 1)
