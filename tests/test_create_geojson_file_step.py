import json
from georef_ar_etl.models import Province
from georef_ar_etl.loaders import CreateGeoJSONFileStep
from . import ETLTestCase


class TestCreateGeoJSONFileStep(ETLTestCase):

    def setUp(self):
        super().setUp()
        self.create_test_provinces(extract=True)

    def test_create_geojson_file(self):
        """El paso debería crear un archivo GeoJSON con los contenidos de la
        tabla especificada."""
        filename = 'test.geojson'
        step = CreateGeoJSONFileStep(Province, filename)
        step.run(None, self._ctx)

        with self._ctx.fs.open(filename) as f:
            data = json.load(f)

        self.assertEqual(data['type'], 'FeatureCollection')
        self.assertEqual(data['features'][0]['properties']['id'], '70')
