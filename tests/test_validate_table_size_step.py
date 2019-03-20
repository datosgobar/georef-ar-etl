from sqlalchemy.sql import sqltypes
from georef_ar_etl.utils import ValidateTableSizeStep
from georef_ar_etl.exceptions import ProcessException
from . import ETLTestCase


class TestValidateTableSizeStep(ETLTestCase):
    def test_size(self):
        """El validador debería retornar la tabla de entrada si su tamaño es el
        correcto."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.Integer)
        ])

        self._ctx.session.add(t1(id=1))

        step = ValidateTableSizeStep(size=1)
        result = step.run(t1, self._ctx)
        self.assertTrue(result is t1)

    def test_size_tolerance(self):
        """El validador debería retornar la tabla de entrada si su tamaño es el
        correcto, con un margen de error."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.Integer)
        ])

        self._ctx.session.add(t1(id=1))
        self._ctx.session.add(t1(id=2))

        step = ValidateTableSizeStep(size=3, tolerance=1)
        result = step.run(t1, self._ctx)
        self.assertTrue(result is t1)

    def test_size_tolerance_error(self):
        """El validador debería retornar un error si la tabla no tiene el
        tamaño deseado."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.Integer)
        ])

        self._ctx.session.add(t1(id=1))

        step = ValidateTableSizeStep(size=3, tolerance=1)
        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)
