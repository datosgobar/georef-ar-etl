import os
from . import ETLTestCase
from georef_ar_etl.transformers import ExtractZipStep


class TestExtractZipStep(ETLTestCase):
    def test_extract_zip(self):
        """El paso debería extraer correctamente un archivo .zip."""
        filename = 'test.zip'
        with open(self.get_test_file_path(filename), 'rb') as f:
            self._ctx.fs.upload(filename, f)

        step = ExtractZipStep()
        directory = step.run(filename, self._ctx)

        self.assertTrue(
            self._ctx.fs.isfile(os.path.join(directory, 'file1.txt')) and
            self._ctx.fs.isfile(os.path.join(directory, 'file2.txt'))
        )
