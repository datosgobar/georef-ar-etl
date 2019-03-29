from .exceptions import ProcessException


class Step:
    def __init__(self, name, reads_input=True):
        self._name = name
        self._reads_input = reads_input

    def run(self, data, ctx):
        return self._run_internal(data, ctx)

    def _run_internal(self, data, ctx):
        raise NotImplementedError()

    @property
    def name(self):
        return self._name

    def reads_input(self):
        return self._reads_input


class CompositeStep(Step):
    def __init__(self, steps):
        super().__init__('multistep')
        self._steps = steps

    def _run_internal(self, data, ctx):
        results = []
        if isinstance(data, list):
            step_inputs = data
        else:
            step_inputs = [data] * len(self._steps)

        for i, step in enumerate(self._steps):
            ctx.report.info('===> Sub-paso #{}: {}'.format(i + 1, step.name))
            results.append(step.run(step_inputs[i], ctx))
            ctx.report.info('Sub-paso finalizado.\n')

        return results

    def __len__(self):
        return len(self._steps)

    def reads_input(self):
        return any(step.reads_input() for step in self._steps)


class Process:
    def __init__(self, name, steps):
        self._name = name
        self._steps = steps

    def run(self, ctx, start=None, end=None):
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
            for i, step in enumerate(self._steps[start - 1:end]):
                ctx.report.info('==> Paso #{}: {}'.format(i + start,
                                                          step.name))
                previous_result = step.run(previous_result, ctx)
                ctx.report.info('Paso finalizado.\n')
        finally:
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
        self._print_title(ctx)
        for i, step in enumerate(self._steps):
            ctx.report.info('{} {}: {}'.format(
                ' ' if step.reads_input() else '>',
                i + 1,
                step.name
            ))

        ctx.report.info('\n')

    def _print_title(self, ctx, separator_width=60):
        ctx.report.info("=" * separator_width)
        ctx.report.info("|" + " " * (separator_width - 2) + "|")

        ctx.report.info("|" + self._name.title().center(
            separator_width - 2) + "|")

        ctx.report.info("|" + " " * (separator_width - 2) + "|")
        ctx.report.info("=" * separator_width + '\n')
