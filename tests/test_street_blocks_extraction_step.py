import random
from georef_ar_etl import constants
from georef_ar_etl.models import StreetBlock, Street
from georef_ar_etl.street_blocks import StreetBlocksExtractionStep
from tests import ETLTestCase

SAN_JUAN_BLOCKS_COUNT = 32629


class TestStreetBlocksExtractionStep(ETLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_test_provinces(extract=True)
        cls.create_test_departments(extract=True)
        cls.create_test_census_localities(extract=True)

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
        """El ID de cada cuadra debería ser igual al ID de su calle, más los
        últimos 7 dígitos de su ogc_fid."""
        query = self._ctx.session.query(self._tmp_blocks).\
            filter(self._tmp_blocks.tipo != constants.STREET_TYPE_OTHER)

        tmp_block = random.choice(query.all())

        ogc_fid = str(tmp_block.id).rjust(7, '0')
        street_id = str(tmp_block.nomencla).ljust(15, '0')

        block_id = street_id + ogc_fid

        step = StreetBlocksExtractionStep()
        blocks = step.run(self._tmp_blocks, self._ctx)
        self._ctx.session.commit()

        self.assertTrue(bool(self._ctx.session.query(blocks).get(block_id)))
