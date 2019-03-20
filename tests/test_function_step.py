from unittest.mock import Mock
from . import ETLTestCase
from georef_ar_etl.utils import FunctionStep, FirstResultStep


class TestFunctionStep(ETLTestCase):
    def test_fn(self):
        """El paso debería ejecutar la función especificada."""
        fn = Mock()
        step = FunctionStep(fn)
        step.run(None, self._ctx)
        fn.assert_called_once()


class TestFirstResultStep(ETLTestCase):
    def test_result(self):
        """El paso debería retornar el primer elemento de una lista."""
        xs = ['foobar', 'quux']
        result = FirstResultStep.run(xs, self._ctx)
        self.assertTrue(result is xs[0])
