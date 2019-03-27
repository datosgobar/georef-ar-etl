from unittest import mock
from georef_ar_etl.process import Process, Step
from georef_ar_etl.exceptions import ProcessException
from . import ETLTestCase


def get_mock_step(return_value=None, raises_exception=False, reads_input=True):
    step = mock.MagicMock(spec=Step)
    step.run.return_value = return_value
    step.reads_input = reads_input
    if raises_exception:
        step.run.side_effect = ProcessException

    return step


class TestCheckDependenciesStep(ETLTestCase):
    _uses_db = False

    def test_process(self):
        """Un proceso debería ejecutar todos sus pasos y retornar el valor
        final producido."""
        process = Process('test', [
            get_mock_step(reads_input=False),
            get_mock_step(),
            get_mock_step(return_value='test'),
        ])

        result = process.run(self._ctx)
        self.assertEqual(result, 'test')
        for step in process.steps:
            step.run.assert_called_once()

    def test_process_exception(self):
        """Si uno de los pasos lanza una ProcessException, se debería
        interrumpir la ejecución del proceso."""
        process = Process('test', [
            get_mock_step(reads_input=False),
            get_mock_step(raises_exception=True)
        ])

        with self.assertRaises(ProcessException):
            process.run(self._ctx)

    def test_process_exception_initial(self):
        """Si se especifica un paso que requiere datos de entrada como paso
        inicial, se debería interrumpir la ejecución del proceso."""
        process = Process('test', [
            get_mock_step(reads_input=False),
            get_mock_step()
        ])

        with self.assertRaises(ProcessException):
            process.run(self._ctx, start=2)
