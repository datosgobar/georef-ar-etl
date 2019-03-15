from sqlalchemy.sql import sqltypes
from . import ETLTestCase
from georef_ar_etl.utils import ValidateTableSchemaStep
from georef_ar_etl.exceptions import ProcessException


class TestValidateTableSchemaStep(ETLTestCase):
    def test_schema(self):
        """El validador debería retornar la tabla de entrada si su esquema es
        el correcto."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.INTEGER),
            ('nombre', sqltypes.VARCHAR)
        ])

        step = ValidateTableSchemaStep({
            'id': 'integer', 'nombre': 'varchar'
        })

        result = step.run(t1, self._ctx)
        self.assertTrue(result is t1)

    def test_missing_field(self):
        """El validador debería retornar un error si la tabla no contiene una
        columna especificada en el validador."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.INTEGER),
            ('nombre', sqltypes.VARCHAR)
        ])

        step = ValidateTableSchemaStep({
            'id': 'integer', 'nombre': 'varchar', 'geom': 'geometry'
        })

        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)

    def test_extra_field(self):
        """El validador debería retornar un error si la tabla contiene una
        columna no especificada en el validador."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.INTEGER),
            ('nombre', sqltypes.VARCHAR),
            ('tipo', sqltypes.VARCHAR)
        ])

        step = ValidateTableSchemaStep({
            'id': 'integer', 'nombre': 'varchar'
        })

        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)

    def test_wrong_type(self):
        """El validador debería retornar un error si una de las columnas tiene
        un tipo incorrecto."""
        t1 = self.create_table('t1', [
            ('id', sqltypes.INTEGER),
            ('nombre', sqltypes.INTEGER)
        ])

        step = ValidateTableSchemaStep({
            'id': 'integer', 'nombre': 'varchar'
        })

        with self.assertRaises(ProcessException):
            step.run(t1, self._ctx)
