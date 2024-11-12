from georef_ar_etl.models import Locality, CensusLocality, Province
from georef_ar_etl.exceptions import ValidationException
from georef_ar_etl.settlements import SettlementsExtractionStep
from georef_ar_etl.localities import LocalitiesExtractionStep
from . import ETLTestCase
from .test_geometry import TEST_MULTIPOLYGON, TEST_POINT

SAN_JUAN_LOCALITIES_COUNT = 99
TEST_MULTIPOINT = 'SRID=4326;MULTIPOINT((10 40))'


class TestLocalitiesExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_local_governments(extract=True)
        cls.create_test_census_localities(extract=True)

    def setUp(self):
        super().setUp()
        # El proceso de extracción de localidades utiliza la tabla temporal de
        # asentamientos como entrada.
        self._tmp_settlements = self.create_test_settlements()

        # En una corrida normal, el proceso de extracción de asentamientos
        # corre antes que el de localidades, y realiza algunos parches a la
        # tabla temporal de asentamientos. Replicar este comportamiento en el
        # setUp.
        step = SettlementsExtractionStep()
        step.run(self._tmp_settlements, self._ctx)

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(Locality).delete()
        super().tearDown()

    def test_single(self):
        """Las localidades deberían poder ser procesadas desde la tabla
        tmp_localidades e insertadas en la tabla correspondiente
        georef_localidades."""
        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)

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
        step.run(self._tmp_settlements, self._ctx)

        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra=locality_id).\
            update({'nombre_bah': 'LAS LAS FLORES'})

        localities = step.run(self._tmp_settlements, self._ctx)
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
        step.run(self._tmp_settlements, self._ctx)

        # Modificar el ID de un localidad
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': '70049040999'})

        step.run(self._tmp_settlements, self._ctx)
        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['70049040999'])
        self.assertListEqual(report_data['deleted_entities_ids'],
                             ['70049040000'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        locality_id = '70049040000'
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra=locality_id).\
            update({'nombre_bah': '  LAS FLORES   \n\nLAS FLORES2'})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)
        name = self._ctx.session.query(localities).\
            filter_by(id=locality_id).\
            one().nombre
        self.assertEqual(name, 'LAS FLORES')

    def test_id_length(self):
        """No se debería poder crear una localidad con longitud de ID
        inválida."""
        step = LocalitiesExtractionStep()
        locality = self._ctx.session.query(self._tmp_settlements).\
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
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': new_id})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)
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
        self._ctx.session.query(self._tmp_settlements).\
            filter_by(cod_bahra='70049040000').\
            update({'cod_bahra': new_id})

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)
        query = self._ctx.session.query(localities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('localities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_LOCALITIES_COUNT - 1)

    def test_caba_virtual_department(self):
        """Una localidad debería poder pertenecer al departamento '02000',
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
        self._ctx.session.commit()

        census_loc = CensusLocality(
            id='02000010',
            nombre='test',
            categoria='LS',
            funcion=None,
            lon=0, lat=0,
            provincia_id='02',
            departamento_id='02000',
            gobierno_local_id=None,
            fuente='test',
            geometria=TEST_POINT
        )
        self._ctx.session.add(census_loc)
        self._ctx.session.commit()

        new_locality = self._tmp_settlements(
            cod_bahra='02000010000',
            nombre_bah='test',
            tipo_bahra='LS',
            fuente_ubi='test',
            geom=TEST_MULTIPOINT
        )
        self._ctx.session.add(new_locality)
        self._ctx.session.commit()

        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)

        loc = self._ctx.session.query(localities).get('02000010000')
        self.assertTrue(loc.departamento_id is None)

    def test_local_government(self):
        """Si una localidad está geográficamente contenida por un gobierno local, se
        debería establecer ese gobierno local como su propiedad 'gobierno_local_id'."""
        step = LocalitiesExtractionStep()
        localities = step.run(self._tmp_settlements, self._ctx)

        locality = self._ctx.session.query(localities).get('70070050002')
        self.assertEqual(locality.gobierno_local_id, '700070')
