from sqlalchemy.sql import sqltypes
from . import ETLTestCase
from georef_ar_etl.utils import CheckDependenciesStep
from georef_ar_etl.exceptions import ProcessException


class TestCheckDependenciesStep(ETLTestCase):
    def test_empty_dep(self):
        """El paso debería fallar si una de las dependencias (tablas) está
        vacía."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.Integer)
        ])

        step = CheckDependenciesStep([t1])
        with self.assertRaises(ProcessException):
            step.run(None, self._ctx)

    def test_nonempty_dep(self):
        """El paso no debería fallar si todas las dependencias contienen
        elementos."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.Integer)
        ])

        self._ctx.session.add(t1(id=1))

        step = CheckDependenciesStep([t1])
        result = step.run(None, self._ctx)
        self.assertIsNone(result)
