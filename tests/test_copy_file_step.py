from georef_ar_etl.utils import CopyFileStep
from . import ETLTestCase


class TestCopyFileStep(ETLTestCase):
    _uses_db = False

    def test_copy_file(self):
        """El paso debería copiar un archivo correctamente."""
        filename = 'test.txt'
        dst = 'test2.txt'
        self.copy_test_file(filename)

        step = CopyFileStep(dst)
        step.run(filename, self._ctx)

        with self._ctx.fs.open(filename) as f:
            text1 = f.read()

        with self._ctx.fs.open(dst) as f:
            text2 = f.read()

        self.assertEqual(text1, text2)
