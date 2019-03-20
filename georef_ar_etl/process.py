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
        for i, step in enumerate(self._steps):
            ctx.report.info('===> Sub-paso #{}: {}'.format(i + 1, step.name))
            results.append(step.run(data, ctx))
            ctx.report.info('Sub-paso finalizado.\n')

        return results

    def reads_input(self):
        return any(step.reads_input() for step in self._steps)


class Process:
    def __init__(self, name, steps):
        self._name = name
        self._steps = steps

    def run(self, start, ctx):
        self._print_title(ctx.report.logger)
        previous_result = None

        initial = self._steps[start]
        if initial.reads_input():
            raise ProcessException(
                'El paso #{} requiere un valor de entrada.'.format(start + 1))

        try:
            for i, step in enumerate(self._steps[start:]):
                ctx.report.info('==> Paso #{}: {}'.format(i + start + 1,
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

    def _print_title(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.title().center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width + '\n')
