from sqlalchemy.sql import sqltypes
from georef_ar_etl.utils import ValidateTableSizeStep
from georef_ar_etl.exceptions import ProcessException
from . import ETLTestCase


class TestValidateTableSizeStep(ETLTestCase):
    def test_eq(self):
        """El validador debería retornar la tabla de entrada si su tamaño es el
        correcto."""
        t1 = self.create_table('t1', {
            'id': sqltypes.INTEGER
        }, pkey='id')

        self._ctx.session.add(t1(id=1))

        step = ValidateTableSizeStep(target_size=1)
        result = step.run(t1, self._ctx)
        self.assertTrue(result is t1)

    def test_eq_error(self):
        """El validador debería lanzar una excepción si el tamaño no es el
        esperado, cuando se utiliza el operator 'eq'."""
        t1 = self.create_table('t1', {
            'id': sqltypes.INTEGER
        }, pkey='id')

        self._ctx.session.add(t1(id=1))

        step = ValidateTableSizeStep(target_size=2)
        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)

    def test_size_greater_equal_than(self):
        """El validador debería retornar la tabla de entrada si su tamaño es
        mayor o igual al tamaño objetivo, cuando se utiliza el operador
        'ge'."""
        t1 = self.create_table('t1', {
            'id': sqltypes.INTEGER
        }, pkey='id')

        for i in range(10):
            self._ctx.session.add(t1(id=i))

        step = ValidateTableSizeStep(target_size=5, op='ge')
        result = step.run(t1, self._ctx)
        self.assertTrue(result is t1)

    def test_size_greater_equal_than_error(self):
        """El validador debería lanzar una excepción si su tamaño es menor al
        tamaño objetivo, cuando se utiliza el operador 'ge'."""
        t1 = self.create_table('t1', {
            'id': sqltypes.INTEGER
        }, pkey='id')

        for i in range(10):
            self._ctx.session.add(t1(id=i))

        step = ValidateTableSizeStep(target_size=11, op='ge')
        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)
