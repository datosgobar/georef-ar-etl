from sqlalchemy.sql import sqltypes
from . import ETLTestCase
from georef_ar_etl.utils import DropTableStep


class TestDropTableStep(ETLTestCase):
    def test_drop(self):
        """El paso debería eliminar la tabla (DROP)."""
        tbl = self.create_table('tbl', [
            ('id', sqltypes.Integer)
        ])
        self.assertTrue(self._metadata.tables['tbl'].exists(
            bind=self._ctx.engine))

        step = DropTableStep()
        step.run(tbl, self._ctx)
        self.assertFalse(self._metadata.tables['tbl'].exists(
            bind=self._ctx.engine))
