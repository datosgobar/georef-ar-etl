import json
from georef_ar_etl import constants
from georef_ar_etl.models import Province
from georef_ar_etl.loaders import CreateJSONFileStep
from . import ETLTestCase


class TestCopyFileStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self.create_test_provinces(extract=True)

    def test_create_json_file(self):
        """El paso debería crear un archivo JSON con los contenidos de la tabla
        especificada."""
        filename = 'test.json'
        step = CreateJSONFileStep(Province, filename)
        step.run(None, self._ctx)

        with self._ctx.fs.open(filename) as f:
            data = json.load(f)

        self.assertEqual(data['version'], constants.ETL_VERSION)
        self.assertEqual(len(data['datos']), 1)

        entity = data['datos'][0]
        self.assertEqual(entity['id'], '70')
