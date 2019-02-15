class ProcessException(Exception):
    pass


class ETL:
    def __init__(self, name, dependencies):
        self._name = name
        self._dependencies = dependencies

    def run(self, ctx):
        self._print_log_separator(ctx.logger)
        session = ctx.session

        try:
            self._check_dependencies(ctx)
            self._run_internal(ctx)
        except ProcessException as e:
            ctx.logger.error('Sucedió un error en el proceso:')
            ctx.logger.error(e)

        ctx.logger.info('Commit...')
        session.commit()

        ctx.logger.info('')

    @property
    def name(self):
        return self._name

    def _check_dependencies(self, ctx):
        for dep in self._dependencies:
            if not ctx.query(dep).first():
                raise ProcessException(
                    'La tabla "{}" está vacía.'.format(dep.__tablename__))

    def _run_internal(self, ctx):
        raise NotImplementedError()

    def _print_log_separator(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.title().center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width)
        l.info('')
