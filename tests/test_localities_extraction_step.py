from georef_ar_etl.models import Locality
from georef_ar_etl.exceptions import ValidationException
from georef_ar_etl.localities import LocalitiesExtractionStep
from . import ETLTestCase

SAN_JUAN_LOCALITIES_COUNT = 99


class TestLocalitiesExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_municipalities(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_localities = self.create_test_localities()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Locality).delete()
        self._ctx.session.query(self._tmp_localities).delete()
        super().tearDown()

    def test_single(self):
        """Las localidades deberían poder ser procesadas desde la tabla
        tmp_localidades e insertadas en la tabla correspondiente
        georef_localidades."""
        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_localities, self._ctx)

        self.assertEqual(self._ctx.session.query(localities).count(),
                         SAN_JUAN_LOCALITIES_COUNT)

        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_LOCALITIES_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de una localidad (no el ID), luego de la
        extracción el campo nuevo debería figurar en georef_localidades."""
        # Ejecutar la extracción por primera vez
        locality_id = '70049040000'
        step = LocalitiesExtractionStep()
        step.run(self._tmp_localities, self._ctx)

        self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra=locality_id).\
            update({'nombre_bah': 'LAS LAS FLORES'})

        localities = step.run(self._tmp_localities, self._ctx)
        name = self._ctx.session.query(localities).\
            filter_by(id=locality_id).\
            one().nombre
        self.assertEqual(name, 'LAS LAS FLORES')

    def test_id_change(self):
        """Si se modifica el ID de una localidad, se debería eliminar la
        localidad con el ID antiguo y se debería generar una nueva en la
        tabla georef_localidades."""
        # Ejecutar la extracción por primera vez
        step = LocalitiesExtractionStep()
        step.run(self._tmp_localities, self._ctx)

        # Modificar el ID de un localidad
        self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': '70021000099'})

        step.run(self._tmp_localities, self._ctx)
        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['70021000099'])
        self.assertListEqual(report_data['deleted_entities_ids'],
                             ['70049040000'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        locality_id = '70049040000'
        self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra=locality_id).\
            update({'nombre_bah': '  LAS FLORES   \n\nLAS FLORES2'})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_localities, self._ctx)
        name = self._ctx.session.query(localities).\
            filter_by(id=locality_id).\
            one().nombre
        self.assertEqual(name, 'LAS FLORES')

    def test_id_length(self):
        """No se debería poder crear una localidad con longitud de ID
        inválida."""
        step = LocalitiesExtractionStep()
        locality = self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra='70049040000').one()

        self._ctx.session.expunge(locality)
        locality.cod_bahra = '7004904000000'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(locality, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si una localidad hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '99049040000'
        self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': new_id})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_localities, self._ctx)
        query = self._ctx.session.query(localities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_LOCALITIES_COUNT - 1)

    def test_invalid_department(self):
        """Si una localidad hace referencia a un departamento inexistente, se
        debería reportar el error."""
        new_id = '70999040000'
        self._ctx.session.query(self._tmp_localities).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': new_id})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_localities, self._ctx)
        query = self._ctx.session.query(localities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_LOCALITIES_COUNT - 1)

    def test_municipality(self):
        """Si una localidad está geográficamente contenida por un municipio, se
        debería establecer ese municipio como su propiedad 'municipio_id'."""
        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_localities, self._ctx)

        locality = self._ctx.session.query(localities).get('70070050002')
        self.assertEqual(locality.municipio_id, '700070')
