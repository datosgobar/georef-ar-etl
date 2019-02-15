class ETL:
    def __init__(self, name, dependencies):
        self._name = name
        self._dependencies = dependencies

    def run(self, ctx):
        self._print_log_separator(ctx.logger)
        self._check_dependencies(ctx)

        session = ctx.session
        self._run_internal(ctx)

        ctx.logger.info('Commit...')
        session.commit()

        ctx.logger.info('')

    @property
    def name(self):
        return self._name

    def _check_dependencies(self, ctx):
        for dep in self._dependencies:
            if not ctx.query(dep).first():
                ctx.logger.error(
                    'La tabla "%s" está vacía.' % dep.__tablename__)
                ctx.logger.error('No se puede continuar con el ETL actual.')

                raise RuntimeError()

    def _run_internal(self, ctx):
        raise NotImplementedError()

    def _print_log_separator(self, l, separator_width=60):
        l.info("=" * separator_width)
        l.info("|" + " " * (separator_width - 2) + "|")

        l.info("|" + self._name.title().center(separator_width - 2) + "|")

        l.info("|" + " " * (separator_width - 2) + "|")
        l.info("=" * separator_width)
        l.info('')
