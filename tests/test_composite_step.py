from georef_ar_etl.process import CompositeStep
from . import ETLTestCase
from .test_process import get_mock_step


class TestCompositeStep(ETLTestCase):
    _uses_db = False

    def test_composite_step(self):
        """Un CompositeStep debería requerir una entrada si cualquiera de sus
        subpasos requiere una entrada."""
        step_a = CompositeStep([
            get_mock_step(),
            get_mock_step(reads_input=False)
        ])

        step_b = CompositeStep([
            get_mock_step(reads_input=False),
            get_mock_step(reads_input=False)
        ])

        self.assertTrue(step_a.reads_input())
        self.assertFalse(step_b.reads_input())
