import os
from georef_ar_etl.transformers import ExtractZipStep
from . import ETLTestCase


class TestExtractZipStep(ETLTestCase):
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
