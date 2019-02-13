class ETL:
    def __init__(self, name):
        self._name = name

    def run(self, context):
        self._print_log_separator(context.logger)
        session = context.session

        self._run_internal(context)

        context.logger.info('Commit...')
        session.commit()

        context.logger.info('')

    def _run_internal(self, context):
        raise NotImplementedError()

    def _print_log_separator(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width)
        l.info('')
