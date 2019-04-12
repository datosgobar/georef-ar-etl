from abc import ABC, abstractmethod
from .exceptions import ProcessException


class Step(ABC):
    """Representa una acción a ejecutar dentro de un contexto dado.
    Step es una clase abstracta.

    La estructura del ETL Georef se basa en series de Step, que son ejecutados
    en distintas configuraciones, siempre bajo un mismo contexto (Context). Los
    datos generados por cada Step son utilizados por los siguientes para formar
    un 'pipeline' de datos, donde el resultado final es una serie de archivos
    listos para ser indexados por Georef API.

    Attributes:
        _name (str): Nombre del paso.
        _reads_input (bool): Falso si el paso no requiere de un valor de
            entrada para ser ejecutado.

    """

    def __init__(self, name, reads_input=True):
        """Inicializa un objeto de tipo 'Step'.

        Args:
            name (str): Ver atributo '_name'.
            reads_input (bool): Ver atributo '_reads_input'.

        """
        self._name = name
        self._reads_input = reads_input

    def run(self, data, ctx):
        """Ejecuta el paso en un contexto.

        Args:
            data (object): Valor de entrada.
            ctx (Context): Contexto de ejecución.

        Returns:
            object: Resultado de la ejecución.

        """
        # Delegar la ejecución real a _run_internal()
        # Si se desea agregar comportamiento común a todos los Steps,
        # se puede agregar en este método (run()).
        return self._run_internal(data, ctx)

    @abstractmethod
    def _run_internal(self, data, ctx):
        """Ejecuta el paso en un contexto (método abstracto).

        Args:
            data (object): Valor de entrada.
            ctx (Context): Contexto de ejecución.

        Returns:
            object: Resultado de la ejecución.

        """
        raise NotImplementedError()

    @property
    def name(self):
        return self._name

    def reads_input(self):
        return self._reads_input


class CompositeStep(Step):
    """Representa un conjunto de pasos a ser ejecutados utilizando un mismo
    valor de entrada en común. Los pason son ejecutados en el orden en el que
    fueron especificados. Notar que CompositeStep es un Step en sí mismo.

    Attributes:
        _steps (list): Lista de pasos internos.

    """

    def __init__(self, steps, name=None):
        """Inicializa un objeto de tipo 'CompositeStep'.

        Args:
            steps (list): Ver atributo '_steps'.
            name (str): Ver atributo '_name'.

        """
        super().__init__(name or 'composite_step')
        self._steps = steps

    def _run_internal(self, data, ctx):
        """Ejecuta el paso compuesto dentro de un contexto dado.
        Si el valor de entrada es una lista, se ejecuta el subpaso i con el
        valor número i de la lista. Si el valor de entrada no es una lista, se
        ejecuta cada subpaso con ese mismo valor.

        Args:
            data (object): Valor de entrada.
            ctx (Context): Contexto de ejecución.

        Returns:
            list: Lista con resultados de cada paso.

        """
        results = []
        if isinstance(data, list):
            step_inputs = data
        else:
            step_inputs = [data] * len(self._steps)

        ctx.report.increase_indent()

        for i, step in enumerate(self._steps):
            ctx.report.info('===> Sub-paso #{}: {}'.format(i + 1, step.name))
            results.append(step.run(step_inputs[i], ctx))
            ctx.report.info('Sub-paso finalizado. [{}]\n'.format(step.name))

        ctx.report.decrease_indent()

        # Juntar todos los resultados en una lista y devolverlos
        return results

    def __len__(self):
        return len(self._steps)

    def reads_input(self):
        # El paso requiere un valor de entrada si cualquiera de los subpasos
        # lo requiere
        return any(step.reads_input() for step in self._steps)


class StepSequence(Step):
    """Representa una lista de pasos a ser ejecutados en orden, con la salida
    de cada uno utilizado como entrada del siguiente. Notar que StepSequence es
    un Step en sí mismo.

    Attributes:
        _steps (list): Lista de pasos a ejecutar.

    """

    def __init__(self, steps, name=None):
        """Inicializa un objeto de tipo 'StepSequence'.

        Args:
            steps (list): Ver atributo '_steps'.
            name (str): Ver atributo '_name'.

        """
        super().__init__(name or 'step_sequence',
                         reads_input=steps[0].reads_input())
        self._steps = steps

    def _run_internal(self, data, ctx):
        """Ejecuta el StepSequence dentro de un contexto dado.

        Args:
            data (object): Valor de entrada.
            ctx (Context): Contexto de ejecución.

        Returns:
            object: Resultado del último paso de '_steps'.

        """
        ctx.report.increase_indent()

        for i, step in enumerate(self._steps):
            ctx.report.info('===> Sub-paso #{}: {}'.format(i + 1, step.name))
            data = step.run(data, ctx)
            ctx.report.info('Sub-paso finalizado. [{}]\n'.format(step.name))

        ctx.report.decrease_indent()

        return data


class Process:
    """Representa un proceso (secuencia de pasos) a ser ejecutado en un
    contexto.

    Attributes:
        _name (str): Nombre del proceso.
        _steps (list): Lista de pasos a ejecutar.

    """

    def __init__(self, name, steps):
        """Inicializa un objeto de tipo 'Process'.

        Args:
            _name (str): Ver atributo '_name'.
            _steps (str): Ver atributo '_steps'.

        """
        self._name = name
        self._steps = steps

    def run(self, ctx, start=None, end=None):
        """Ejecuta el proceso dentro de un contexto dado. Para lograr esto, se
        ejecuta cada paso de la lista '_steps', tomando la salida de cada paso
        como entrada del siguiente. Al finalizar la ejecución del proceso, se
        realiza un commit() del objeto Session.

        Args:
            ctx (Context): Contexto de ejecución.
            start (int): Índice (desde 1) de paso a utilizar como inicial
                (opcional).
            end (int): Índice (desde 1) de paso a utilizar como final
                (inclusivo) (opcional).

        Returns:
            object: Resultado de la ejecución (último paso).

        """
        self._print_title(ctx)
        previous_result = None
        start = start or 1
        end = len(self._steps) if end is None else end

        if start < 1 or end < 1 or end < start:
            raise ProcessException('Rango de pasos mal formado: {}-{}.'.format(
                start, end))

        if start > len(self._steps) or end > len(self._steps):
            raise ProcessException('Rango de pasos inválido: {}-{}.'.format(
                start, end))

        initial = self._steps[start - 1]
        if initial.reads_input():
            raise ProcessException(
                'El paso #{} ({}) requiere un valor de entrada.'.format(
                    start, initial.name))

        try:
            # Ejecutar cada paso tomando la salida del paso anterior y
            # utilizándola como entrada para el paso siguiente
            for i, step in enumerate(self._steps[start - 1:end]):
                ctx.report.info('==> Paso #{}: {}'.format(i + start,
                                                          step.name))
                previous_result = step.run(previous_result, ctx)
                ctx.report.info('Paso finalizado. [{}]\n'.format(step.name))
        except Exception:
            ctx.report.reset_indent()
            ctx.report.error('Realizando Rollback...')
            ctx.session.rollback()
            raise

        ctx.report.info('Commit...')
        ctx.session.commit()

        ctx.report.info('Ejecución de proceso finalizada.\n')
        return previous_result

    @property
    def name(self):
        return self._name

    @property
    def steps(self):
        return self._steps

    def print_info(self, ctx):
        """Agrega información del proceso a un reporte en un contexto.

        Args:
            ctx (Context): Contexto de ejecución.

        """
        self._print_title(ctx)
        for i, step in enumerate(self._steps):
            ctx.report.info('{} {}: {}'.format(
                ' ' if step.reads_input() else '>',
                i + 1,
                step.name
            ))

        ctx.report.info('\n')

    def _print_title(self, ctx, separator_width=60):
        """Agrega un separador de texto a un reporte en un contexto.

        Args:
            ctx (Context): Contexto de ejecución.
            separator_width (int): Ancho del separador en caracteres.

        """
        ctx.report.info("=" * separator_width)
        ctx.report.info("|" + " " * (separator_width - 2) + "|")

        ctx.report.info("|" + self._name.title().replace('_', ' ').center(
            separator_width - 2) + "|")

        ctx.report.info("|" + " " * (separator_width - 2) + "|")
        ctx.report.info("=" * separator_width + '\n')
