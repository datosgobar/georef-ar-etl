class ETL:
    def __init__(self, name):
        self._name = name

    def run(self, ctx):
        self._print_log_separator(ctx.logger)
        session = ctx.session

        self._run_internal(ctx)

        ctx.logger.info('Commit...')
        session.commit()

        ctx.logger.info('')

    @property
    def name(self):
        return self._name

    def _run_internal(self, ctx):
        raise NotImplementedError()

    def _print_log_separator(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.title().center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width)
        l.info('')
