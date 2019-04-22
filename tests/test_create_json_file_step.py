import json
from georef_ar_etl.models import Province
from georef_ar_etl.loaders import CreateJSONFileStep
from . import ETLTestCase


class TestCreateJSONFileStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self.create_test_provinces(extract=True)

    def test_create_json_file(self):
        """El paso debería crear un archivo JSON con los contenidos de la tabla
        especificada."""
        filename = 'provincias.json'
        step = CreateJSONFileStep(Province, filename)
        step.run(None, self._ctx)

        with self._ctx.fs.open(filename) as f:
            data = json.load(f)

        self.assertEqual(data['inicio'], 0)
        self.assertEqual(data['total'], 1)
        self.assertEqual(data['cantidad'], 1)
        self.assertEqual(len(data['provincias']), 1)

        entity = data['provincias'][0]
        self.assertEqual(entity['id'], '70')
