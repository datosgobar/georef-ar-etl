import json
from georef_ar_etl.json_stream_writer import JSONStreamWriter, \
    JSONArrayPlaceholder
from . import ETLTestCase


class TestJSONStreamWriter(ETLTestCase):
    _uses_db = False

    def test_no_placeholder(self):
        """El inicializador de JSONStreamWriter debería lanzar una excepción si
        el template no contiene un JSONArrayPlaceholder."""
        with self.assertRaises(ValueError):
            self.assert_json_file({}, [], {})

    def test_list_default_template(self):
        """El JSONStreamWriter debería escribir el template y todos los valores
        de la lista de ítems, utilizando el template default: [])"""
        data = [1, 2, 3, 4, 5]
        self.assert_json_file(data, data)

    def test_list(self):
        """El JSONStreamWriter debería escribir el template y todos los valores
        de la lista de ítems."""
        values = [1, 2, 3, 4, 5]
        template = {
            'test': 'foobar',
            'values': JSONArrayPlaceholder()
        }
        data = {
            'test': 'foobar',
            'values': values
        }
        self.assert_json_file(data, values, template)

    def test_list_varied(self):
        """El JSONStreamWriter debería escribir el template y todos los valores
        de la lista de ítems, sin importar el tipo de cada ítem (mientras sean
        JSON serializable)."""
        values = [1, True, {'foo': 'bar'}]
        template = {
            'test': 'foobar',
            'values': {
                'nested': JSONArrayPlaceholder()
            }
        }
        data = {
            'test': 'foobar',
            'values': {
                'nested': values
            }
        }
        self.assert_json_file(data, values, template)

    def assert_json_file(self, full_data, items, template=None):
        filename = 'test.json'
        with self._ctx.fs.open(filename, 'w') as f:
            with JSONStreamWriter(f, template=template, bufsize=1) as w:
                for item in items:
                    w.append(item)

        with self._ctx.fs.open(filename) as f:
            loaded_full_data = json.load(f)

        if template is not None:
            self.assertDictEqual(loaded_full_data, full_data)
        else:
            self.assertListEqual(loaded_full_data, full_data)
