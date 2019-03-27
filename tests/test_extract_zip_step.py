import os
from georef_ar_etl.exceptions import ProcessException
from georef_ar_etl.transformers import ExtractZipStep
from . import ETLTestCase


class TestExtractZipStep(ETLTestCase):
    _uses_db = False

    def test_extract_zip(self):
        """El paso debería extraer correctamente un archivo .zip."""
        filename = 'test.zip'
        self.copy_test_file(filename)

        step = ExtractZipStep()
        directory = step.run(filename, self._ctx)

        self.assertTrue(
            self._ctx.fs.isfile(os.path.join(directory, 'file1.txt')) and
            self._ctx.fs.isfile(os.path.join(directory, 'file2.txt'))
        )

    def test_extract_error(self):
        """El paso debería lanzar ProcessException si el archivo no es
        válido."""
        filename = 'test.txt'
        self.copy_test_file(filename)

        step = ExtractZipStep()

        with self.assertRaises(ProcessException):
            step.run(filename, self._ctx)
