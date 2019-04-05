import csv
import random
from georef_ar_etl.models import Department
from georef_ar_etl.loaders import CreateCSVFileStep
from . import ETLTestCase
from .test_departments_extraction_step import SAN_JUAN_DEPT_COUNT


class TestCreateGeoJSONFileStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self.create_test_provinces(extract=True)
        self.create_test_departments(extract=True)

    def test_create_csv_file(self):
        """El paso debería crear un archivo CSV con los contenidos de la
        tabla especificada."""
        filename = 'test.csv'
        step = CreateCSVFileStep(Department, filename)
        step.run(None, self._ctx)

        with self._ctx.fs.open(filename, newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        self.assertEqual(len(rows), SAN_JUAN_DEPT_COUNT)
        row = random.choice(rows)

        self.assertFalse('geometria' in row)
