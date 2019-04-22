import json
from georef_ar_etl.models import Department
from georef_ar_etl.constants import ETL_VERSION
from georef_ar_etl.loaders import CreateNDJSONFileStep
from . import ETLTestCase


class TestCreateNDJSONFileStep(ETLTestCase):
    def setUp(self):
        super().setUp()
        self.create_test_provinces(extract=True)
        self.create_test_departments(extract=True)

    def test_create_ndjson_file(self):
        """El paso debería crear un archivo NDJSON (http://ndjson.org/) con los
        contenidos de la tabla especificada."""
        filename = 'departamentos.json'
        step = CreateNDJSONFileStep(Department, filename)
        step.run(None, self._ctx)

        entities = []

        with self._ctx.fs.open(filename) as f:
            first = True

            for line in f:
                data = json.loads(line)
                if first:
                    metadata = data
                    first = False
                else:
                    entities.append(data)

        self.assertEqual(metadata['version'], ETL_VERSION)
        self.assertEqual(len(entities), metadata['cantidad'])

        entity = entities[0]
        self.assertEqual(entity['provincia']['id'], '70')
