from georef_ar_etl.process import StepSequence
from . import ETLTestCase
from .test_process import get_mock_step


class TestStepSequence(ETLTestCase):
    _uses_db = False

    def test_sequence_step(self):
        """Un StepSequence debería requerir entrada si su primer subpaso
        requiere entrada."""
        step_a = StepSequence([
            get_mock_step(),
            get_mock_step(reads_input=False)
        ])

        step_b = StepSequence([
            get_mock_step(reads_input=False),
            get_mock_step(reads_input=False)
        ])

        self.assertTrue(step_a.reads_input())
        self.assertFalse(step_b.reads_input())

    def test_step_sequence_output(self):
        """La salida de un StepSequence debería ser igual a la de su último
        subpaso."""
        step_a = StepSequence([
            get_mock_step(reads_input=False, return_value=1),
            get_mock_step(return_value=2),
            get_mock_step(return_value=3)
        ])

        self.assertEqual(step_a.run(None, self._ctx), 3)
