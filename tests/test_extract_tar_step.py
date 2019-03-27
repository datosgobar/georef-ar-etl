import os
from georef_ar_etl.exceptions import ProcessException
from georef_ar_etl.transformers import ExtractTarStep
from . import ETLTestCase


class TestExtractTarStep(ETLTestCase):
    _uses_db = False

    def test_extract_tar(self):
        """El paso debería extraer correctamente un archivo .tar.gz."""
        filename = 'test.tar.gz'
        self.copy_test_file(filename)

        step = ExtractTarStep()
        directory = step.run(filename, self._ctx)

        self.assertTrue(
            self._ctx.fs.isfile(os.path.join(directory, 'file3.txt')) and
            self._ctx.fs.isfile(os.path.join(directory, 'file4.txt'))
        )

    def test_extract_error(self):
        """El paso debería lanzar ProcessException si el archivo no es
        válido."""
        filename = 'test.txt'
        self.copy_test_file(filename)

        step = ExtractTarStep()

        with self.assertRaises(ProcessException):
            step.run(filename, self._ctx)
