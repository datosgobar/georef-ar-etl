import os
from . import ETLTestCase
from georef_ar_etl.transformers import ExtractTarStep


class TestExtractTarStep(ETLTestCase):
    def test_extract_tar(self):
        """El paso debería extraer correctamente un archivo .tar."""
        filename = 'test.tar.gz'
        with open(self.get_test_file_path(filename), 'rb') as f:
            self._ctx.fs.upload(filename, f)

        step = ExtractTarStep()
        directory = step.run(filename, self._ctx)

        self.assertTrue(
            self._ctx.fs.isfile(os.path.join(directory, 'file3.txt')) and
            self._ctx.fs.isfile(os.path.join(directory, 'file4.txt'))
        )
