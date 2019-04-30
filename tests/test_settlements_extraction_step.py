from georef_ar_etl.models import Settlement, Province
from georef_ar_etl.exceptions import ValidationException, ProcessException
from georef_ar_etl.settlements import SettlementsExtractionStep
from . import ETLTestCase
from .test_geometry import TEST_MULTIPOLYGON

SAN_JUAN_SETTLEMENTS_COUNT = 216
TEST_MULTIPOINT = 'SRID=4326;MULTIPOINT((10 40))'


class TestSettlementsExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_municipalities(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_settlements = self.create_test_settlements()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Settlement).delete()
        super().tearDown()

    def test_single(self):
        """Los asentamientos deberían poder ser procesadas desde la tabla
        tmp_asentamientos e insertados en la tabla correspondiente
        georef_asentamientos."""
        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)

        self.assertEqual(self._ctx.session.query(settlements).count(),
                         SAN_JUAN_SETTLEMENTS_COUNT)

        report_data = self._ctx.report.get_data('settlements_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_SETTLEMENTS_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de un asentamiento (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_asentamientos."""
        # Ejecutar la extracción por primera vez
        settlement_id = '70056000081'
        step = SettlementsExtractionStep()
        step.run(self._tmp_settlements, self._ctx)

        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra=settlement_id).\
            update({'nombre_bah': 'LAS LAS AGUADITAS'})

        settlements = step.run(self._tmp_settlements, self._ctx)
        name = self._ctx.session.query(settlements).\
            filter_by(id=settlement_id).\
            one().nombre
        self.assertEqual(name, 'LAS LAS AGUADITAS')

    def test_id_change(self):
        """Si se modifica el ID de un asentamiento, se debería eliminar el
        asentamiento con el ID antiguo y se debería generar uno nuevo en la
        tabla georef_asentamientos."""
        # Ejecutar la extracción por primera vez
        step = SettlementsExtractionStep()
        step.run(self._tmp_settlements, self._ctx)

        # Modificar el ID de un asentamiento
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70056000081').\
            update({'cod_bahra': '70056000099'})

        step.run(self._tmp_settlements, self._ctx)
        report_data = self._ctx.report.get_data('settlements_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['70056000099'])
        self.assertListEqual(report_data['deleted_entities_ids'],
                             ['70056000081'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        settlement_id = '70056000081'
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra=settlement_id).\
            update({'nombre_bah': 'LAS AGUADITAS    '})

        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)
        name = self._ctx.session.query(settlements).\
            filter_by(id=settlement_id).\
            one().nombre
        self.assertEqual(name, 'LAS AGUADITAS')

    def test_id_length(self):
        """No se debería poder crear un asentamiento con longitud de ID
        inválida."""
        step = SettlementsExtractionStep()
        settlement = self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70056000081').one()

        self._ctx.session.expunge(settlement)
        settlement.cod_bahra = '70056000'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(settlement, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si un asentamiento hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '11056000081'
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70056000081').\
            update({'cod_bahra': new_id})

        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)
        query = self._ctx.session.query(settlements).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('settlements_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_SETTLEMENTS_COUNT - 1)

    def test_invalid_department(self):
        """Si un asentamiento hace referencia a un departamento inexistente, se
        debería reportar el error."""
        new_id = '70123000081'
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70056000081').\
            update({'cod_bahra': new_id})

        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)
        query = self._ctx.session.query(settlements).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('settlements_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_SETTLEMENTS_COUNT - 1)

    def test_caba_virtual_department(self):
        """Un asentamiento debería poder pertenecer al departamento '02000',
        aunque el mismo no exista en la base de datos (ver comentario en
        constants.py)."""
        prov = Province(
            id='02',
            nombre='test',
            nombre_completo='test',
            lat=0, lon=0,
            iso_id='test',
            iso_nombre='test',
            categoria='test',
            fuente='test',
            geometria=TEST_MULTIPOLYGON
        )
        self._ctx.session.add(prov)

        new_settlement = self._tmp_settlements(
            cod_bahra='02000010000',
            nombre_bah='test',
            tipo_bahra='LS',
            fuente_ubi='test',
            geom=TEST_MULTIPOINT
        )
        self._ctx.session.add(new_settlement)
        self._ctx.session.commit()

        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)

        loc = self._ctx.session.query(settlements).get('02000010000')
        self.assertTrue(loc.departamento_id is None)

    def test_municipality(self):
        """Si un asentamiento está geográficamente contenido por un municipio,
        se debería establecer ese municipio como su propiedad
        'municipio_id'."""
        step = SettlementsExtractionStep()
        settlements = step.run(self._tmp_settlements, self._ctx)

        settlement = self._ctx.session.query(settlements).get('70056000081')
        self.assertEqual(settlement.municipio_id, '700056')

    def test_invalid_commune(self):
        """Si un asentamiento de CABA tiene un ID de departamento mayor a 15,
        debería generarse un error. Ver comentario en constants.py para más
        información."""
        self._ctx.session.add(self._tmp_settlements(cod_bahra='02016000081'))
        step = SettlementsExtractionStep()

        with self.assertRaises(ProcessException):
            step.run(self._tmp_settlements, self._ctx)
