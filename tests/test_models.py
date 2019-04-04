from georef_ar_etl.models import Province, Department
from . import ETLTestCase
from .test_departments_extraction_step import SAN_JUAN_DEPT_COUNT


class TestModels(ETLTestCase):
    def test_parent_delete(self):
        """Al borrar una entidad pariente, se deberían eliminar las entidades
        que refieran a ella."""
        self.create_test_provinces(extract=True)
        self.create_test_departments(extract=True)

        self.assertEqual(self._ctx.session.query(Department).count(),
                         SAN_JUAN_DEPT_COUNT)

        self._ctx.session.query(Province).delete()
        self.assertEqual(self._ctx.session.query(Department).count(), 0)
