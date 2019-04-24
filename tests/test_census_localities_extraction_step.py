from georef_ar_etl.models import CensusLocality, Province
from georef_ar_etl.exceptions import ValidationException
from georef_ar_etl.census_localities import CensusLocalitiesExtractionStep
from . import ETLTestCase
from .test_geometry import TEST_MULTIPOLYGON

SAN_JUAN_CENSUS_LOCALITIES_COUNT = 82
TEST_POINT = 'SRID=4326;POINT(10 40)'


class TestCensusLocalitiesExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_municipalities(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_census_localities = self.create_test_census_localities()

    def tearDown(self):
        self._ctx.session.commit()
        self._ctx.session.query(CensusLocality).delete()
        self._ctx.session.query(self._tmp_census_localities).delete()
        super().tearDown()

    def test_single(self):
        """Las localidades censales deberían poder ser procesadas desde la
        tabla tmp_localidades_censales e insertadas en la tabla correspondiente
        georef_localidades_censales."""
        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)

        self.assertEqual(self._ctx.session.query(census_localities).count(),
                         SAN_JUAN_CENSUS_LOCALITIES_COUNT)

        report_data = self._ctx.report.get_data('census_localities_extraction')
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_CENSUS_LOCALITIES_COUNT)

    def test_field_change(self):
        """Si se modifica un campo de una localidad censal (no el ID), luego de
        la extracción el campo nuevo debería figurar en
        georef_localidades_censales."""
        # Ejecutar la extracción por primera vez
        census_locality_id = '70091060'
        step = CensusLocalitiesExtractionStep()
        step.run(self._tmp_census_localities, self._ctx)

        self._ctx.session.query(self._tmp_census_localities).\
            filter_by(link=census_locality_id).\
            update({'localidad': 'Don Bosco'})

        localities = step.run(self._tmp_census_localities, self._ctx)
        name = self._ctx.session.query(localities).\
            filter_by(id=census_locality_id).\
            one().nombre
        self.assertEqual(name, 'Don Bosco')

    def test_id_change(self):
        """Si se modifica el ID de una localidad censal, se debería eliminar la
        localidad censal con el ID antiguo y se debería generar una nueva en la
        tabla georef_localidades_censales."""
        # Ejecutar la extracción por primera vez
        step = CensusLocalitiesExtractionStep()
        step.run(self._tmp_census_localities, self._ctx)

        # Modificar el ID de una localidad censal
        self._ctx.session.query(self._tmp_census_localities).\
            filter_by(link='70091060').\
            update({'link': '70091099'})

        step.run(self._tmp_census_localities, self._ctx)
        report_data = self._ctx.report.get_data(
            'census_localities_extraction')
        self.assertListEqual(report_data['new_entities_ids'], ['70091099'])
        self.assertListEqual(report_data['deleted_entities_ids'],
                             ['70091060'])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        census_locality_id = '70091060'
        self._ctx.session.query(self._tmp_census_localities).\
            filter_by(link=census_locality_id).\
            update({'localidad': '  LAS FLORES   \n\nLAS FLORES2'})

        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)
        name = self._ctx.session.query(census_localities).\
            filter_by(id=census_locality_id).\
            one().nombre
        self.assertEqual(name, 'LAS FLORES')

    def test_id_length(self):
        """No se debería poder crear una localidad censal con longitud de ID
        inválida."""
        step = CensusLocalitiesExtractionStep()
        census_locality = self._ctx.session.\
            query(self._tmp_census_localities).\
            filter_by(link='70091060').one()

        self._ctx.session.expunge(census_locality)
        census_locality.link = '700910600000'

        # pylint: disable=protected-access
        with self.assertRaises(ValidationException):
            step._process_entity(census_locality, self._ctx.cached_session(),
                                 self._ctx)

    def test_invalid_province(self):
        """Si una localidad censal hace referencia a una provincia inexistente,
        se debería reportar el error."""
        new_id = '01091060'
        self._ctx.session.query(self._tmp_census_localities).\
            filter_by(link='70091060').\
            update({'link': new_id})

        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)
        query = self._ctx.session.query(census_localities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('census_localities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_CENSUS_LOCALITIES_COUNT - 1)

    def test_invalid_department(self):
        """Si una localidad censal hace referencia a un departamento
        inexistente, se debería reportar el error."""
        new_id = '70555060'
        self._ctx.session.query(self._tmp_census_localities).\
            filter_by(link='70091060').\
            update({'link': new_id})

        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)
        query = self._ctx.session.query(census_localities).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('census_localities_extraction')
        self.assertEqual(len(report_data['errors']), 1)
        self.assertEqual(len(report_data['new_entities_ids']),
                         SAN_JUAN_CENSUS_LOCALITIES_COUNT - 1)

    def test_caba_virtual_department(self):
        """Una localidad censal debería poder pertenecer al departamento
        '02000', aunque el mismo no exista en la base de datos (ver comentario
        en constants.py)."""
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

        new_census_locality = self._tmp_census_localities(
            link='02000000',
            localidad='test',
            func_loc='0',
            tiploc='1',
            geom=TEST_POINT
        )
        self._ctx.session.add(new_census_locality)
        self._ctx.session.commit()

        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)

        loc = self._ctx.session.query(census_localities).get('02000000')
        self.assertTrue(loc.departamento_id is None)

    def test_municipality(self):
        """Si una localidad censal está geográficamente contenida por un
        municipio, se debería establecer ese municipio como su propiedad
        'municipio_id'."""
        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)

        census_locality = self._ctx.session.query(census_localities).get(
            '70091060')
        self.assertEqual(census_locality.municipio_id, '700091')

    def test_administrative_function(self):
        """La capital de la provincia debería tener funcion ==
        CAPITAL_PROVINCIA."""
        step = CensusLocalitiesExtractionStep()
        census_localities = step.run(self._tmp_census_localities, self._ctx)

        census_locality = self._ctx.session.query(census_localities).get(
            '70028010')
        self.assertEqual(census_locality.funcion, 'CAPITAL_PROVINCIA')
