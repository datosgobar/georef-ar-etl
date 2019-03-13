class ProcessException(Exception):
    pass


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
            ctx.logger.info('')
            ctx.logger.info('===> Sub-paso #{}: {}'.format(i + 1, step.name))
            results.append(step.run(data, ctx))

        return results

    def reads_input(self):
        return any(step.reads_input() for step in self._steps)


class Process:
    def __init__(self, name, steps):
        self._name = name
        self._steps = steps

    def run(self, start, ctx):
        self._print_log_separator(ctx.logger)
        session = ctx.session
        previous_result = None

        initial = self._steps[start]
        if initial.reads_input():
            ctx.logger.error('')
            ctx.logger.error(
                'El paso #{} requiere un valor de entrada.'.format(start + 1))
            ctx.logger.error('Utilizar otro paso como paso inicial.')
            ctx.logger.error('')
            return previous_result

        try:
            for i, step in enumerate(self._steps[start:]):
                ctx.logger.info('==> Paso #{}: {}'.format(i + start + 1,
                                                          step.name))
                previous_result = step.run(previous_result, ctx)
                ctx.logger.info('')

        except ProcessException:
            ctx.logger.error('')
            ctx.logger.error(
                'Sucedió un error durante la ejecución del proceso:')
            ctx.logger.exception('Excepción:')
            ctx.logger.error('')

        ctx.logger.info('Commit...')
        session.commit()
        ctx.logger.info('Ejecución de proceso finalizada.')
        ctx.logger.info('')

        return previous_result

    @property
    def name(self):
        return self._name

    def _print_log_separator(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.title().center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width)
        l.info('')
