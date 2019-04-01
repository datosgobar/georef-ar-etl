from georef_ar_etl.models import Street
from georef_ar_etl.exceptions import ProcessException
from georef_ar_etl.streets import StreetsExtractionStep
from . import ETLTestCase

SAN_JUAN_STREETS_COUNT = 754
TEST_MULTILINESTRING = 'SRID=4326;MULTILINESTRING((10 10, 20 20, 10 40),' + \
    '(40 40, 30 30, 40 20, 30 10))'


class TestStreetsExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_blocks = self.create_test_blocks()

    def tearDown(self):
        self._ctx.session.commit()
        # No es necesario borrar tmp_blocks ya que self.create_test_blocks()
        # borra las cuadras temporales (overwrite=True)
        self._ctx.session.query(Street).delete()
        super().tearDown()

    def test_simple(self):
        """Las calles deberían ser generadas a partir de los datos de cuadras
        de cada provincia, e insertadas en la tabla georef_calles."""
        step = StreetsExtractionStep()
        streets = step.run(self._tmp_blocks, self._ctx)

        self.assertEqual(self._ctx.session.query(streets).count(),
                         SAN_JUAN_STREETS_COUNT)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertListEqual(report_data['errors'], [])

    def test_clean_string(self):
        """Los campos de texto deberían ser normalizados en el proceso de
        normalización."""
        street_id = '7003501000900'
        self._ctx.session.query(self._tmp_blocks).\
            filter_by(nomencla=street_id).\
            update({'nombre': '  LAPRIDA   \n\nLAPRIDA\n'})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_blocks, self._ctx)
        name = self._ctx.session.query(streets).\
            filter_by(id=street_id).\
            one().nombre
        self.assertEqual(name, 'LAPRIDA')

    def test_invalid_province(self):
        """Si una cuadra hace referencia a una provincia inexistente, se
        debería reportar el error."""
        new_id = '7703501000900'
        self._ctx.session.query(self._tmp_blocks).\
            filter_by(nomencla='7003501000900').\
            update({'nomencla': new_id})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_blocks, self._ctx)
        query = self._ctx.session.query(streets).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertEqual(len(report_data['errors']), 1)

    def test_invalid_department(self):
        """Si una cuadra hace referencia a un departamento inexistente, se
        debería reportar el error."""
        new_id = '7011101000900'
        self._ctx.session.query(self._tmp_blocks).\
            filter_by(nomencla='7003501000900').\
            update({'nomencla': new_id})

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_blocks, self._ctx)
        query = self._ctx.session.query(streets).filter_by(id=new_id)
        self.assertEqual(query.count(), 0)

        report_data = self._ctx.report.get_data('streets_extraction')
        self.assertEqual(len(report_data['errors']), 1)

    def test_invalid_commune(self):
        """Si una cuadra de CABA tiene un ID de departamento no divisible por
        7, debería generarse un error. Ver comentario en constants.py para más
        información."""
        self._ctx.session.add(self._tmp_blocks(ogc_fid=9999,
                                               nomencla='0200200000000'))
        step = StreetsExtractionStep()

        with self.assertRaises(ProcessException):
            step.run(self._tmp_blocks, self._ctx)

    def test_street_numbers(self):
        """La altura de una calle debería comenzar en la altura más baja de
        todas sus cuadras, y terminar el la mas alta de todas sus cuadras."""
        block_nums = [
            ('7003500000000', '0', '100'),
            ('7003500000000', '101', '200'),
            ('7003500000001', '9', '200'),
            ('7003500000001', '201', '1000')
        ]

        for i, block in enumerate(block_nums):
            self._ctx.session.add(self._tmp_blocks(ogc_fid=9999 + i,
                                                   nomencla=block[0],
                                                   desdei=block[1],
                                                   hastad=block[2],
                                                   tipo='CALLE',
                                                   nombre='test',
                                                   geom=TEST_MULTILINESTRING))
        self._ctx.session.commit()

        step = StreetsExtractionStep()
        streets = step.run(self._tmp_blocks, self._ctx)

        street1 = self._ctx.session.query(streets).get('7003500000000')
        street2 = self._ctx.session.query(streets).get('7003500000001')

        self.assertEqual(street1.inicio_izquierda, 0)
        self.assertEqual(street1.fin_derecha, 200)
        self.assertEqual(street2.inicio_izquierda, 9)
        self.assertEqual(street2.fin_derecha, 1000)
