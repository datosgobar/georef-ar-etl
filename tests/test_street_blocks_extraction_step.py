import random
from georef_ar_etl import constants
from georef_ar_etl.models import StreetBlock, Street
from georef_ar_etl.street_blocks import StreetBlocksExtractionStep
from . import ETLTestCase

SAN_JUAN_BLOCKS_COUNT = 3203


class TestStreetBlocksExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)

    def setUp(self):
        super().setUp()
        self._tmp_blocks = self.create_test_blocks()
        self.extract_streets(self._tmp_blocks)

    def tearDown(self):
        self._ctx.session.commit()
        # No es necesario borrar tmp_blocks ya que self.create_test_blocks()
        # borra las cuadras temporales (overwrite=True)
        self._ctx.session.query(StreetBlock).delete()
        self._ctx.session.query(Street).delete()
        super().tearDown()

    def test_simple(self):
        """Las cuadras deberían ser generadas a partir de los datos de cuadras
        de cada provincia, e insertadas en la tabla georef_cuadras."""
        step = StreetBlocksExtractionStep()
        blocks = step.run(self._tmp_blocks, self._ctx)

        self.assertEqual(self._ctx.session.query(blocks).count(),
                         SAN_JUAN_BLOCKS_COUNT)

    def test_id(self):
        """El ID de cada cuadra debería ser igual a el ID de su calle, más los
        últimos 5 dígitos de su ogc_fid."""
        query = self._ctx.session.query(self._tmp_blocks).\
            filter(self._tmp_blocks.tipo != constants.STREET_TYPE_OTHER)

        tmp_block = random.choice(query.all())

        ogc_fid = str(tmp_block.ogc_fid).rjust(5, '0')
        block_id = tmp_block.nomencla + ogc_fid[-5:]

        step = StreetBlocksExtractionStep()
        blocks = step.run(self._tmp_blocks, self._ctx)

        self.assertTrue(bool(self._ctx.session.query(blocks).get(block_id)))
