import argparse
import code
from fs import osfs
from .exceptions import ProcessException
from .context import Context, Report, RUN_MODES
from . import read_config, get_logger, create_engine, constants
from . import provinces, departments, municipalities, localities
from . import streets, intersections, street_blocks
from . import synonyms, excluding_terms

PROCESSES = [
    constants.PROVINCES,
    constants.DEPARTMENTS,
    constants.MUNICIPALITIES,
    constants.LOCALITIES,
    constants.STREETS,
    constants.INTERSECTIONS,
    constants.STREET_BLOCKS,
    constants.SYNONYMS,
    constants.EXCLUDING_TERMS
]

MODULES = [
    provinces,
    departments,
    municipalities,
    localities,
    streets,
    intersections,
    street_blocks,
    synonyms,
    excluding_terms
]

COMMANDS = [
    'etl',
    'console',
    'info'
]


def parse_args():
    parser = argparse.ArgumentParser(
        prog='georef_ar_etl',
        description='ETL para Georef. Versión: {}.'.format(
            constants.ETL_VERSION)
    )
    parser.add_argument('-p', '--processes', action='append',
                        choices=PROCESSES, help='Procesos ETL a ejecutar.')
    parser.add_argument('-m', '--mode', choices=RUN_MODES, default='normal',
                        help='Modo de ejecución de ETL.')
    parser.add_argument('-s', '--start', default=1, type=int,
                        help='Paso desde el cual comenzar (número).')
    parser.add_argument('-e', '--end', default=None, type=int,
                        help='Paso en el cual terminar (número) (inclusivo).')
    parser.add_argument('-c', '--command', choices=COMMANDS, default='etl',
                        help='Comando a ejecutar.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Imprimir información adicional.')

    return parser.parse_args()


def etl(enabled_processes, start, end, ctx):
    ctx.report.info('Georef ETL')
    ctx.report.info('Versión: {}'.format(constants.ETL_VERSION) + '\n')

    processes = [module.create_process(ctx.config) for module in MODULES]

    for process in processes:
        if not enabled_processes or process.name in enabled_processes:
            try:
                process.run(ctx, start, end)
            except ProcessException:
                ctx.report.exception(
                    'Ocurrió un error durante la ejecución del proceso:')
                ctx.report.info('Continuando...')
            except Exception:  # pylint: disable=broad-except
                ctx.report.exception('Ocurrió un error desconocido:')
                ctx.report.info('Interrumpiendo la ejecución de procesos.')
                break

    ctx.report.write(ctx.config['etl']['reports_dir'])

    if ctx.config.getboolean('mailer', 'enabled'):
        ctx.report.info('Enviando mail...')
        recipients = [
            r.strip()
            for r in ctx.config['mailer']['recipients'].split(',')
        ]

        ctx.report.email(
            host=ctx.config['mailer']['host'],
            user=ctx.config['mailer']['user'],
            password=ctx.config['mailer']['password'],
            recipients=recipients,
            environment=ctx.config['etl']['environment'],
            ssl=ctx.config.getboolean('mailer', 'ssl'),
            port=ctx.config.getint('mailer', 'port')
        )

        ctx.report.info('Mail enviado.')


# pylint: disable=unused-argument
def console(ctx):
    repl = code.InteractiveConsole(locals=locals())
    repl.push('from georef_ar_etl import models')
    repl.interact()


def info(ctx):
    processes = [module.create_process(ctx.config) for module in MODULES]

    ctx.report.info('">" = Punto de entrada válido del proceso.\n')
    for process in processes:
        process.print_info(ctx)


def main():
    args = parse_args()
    config = read_config()
    logger, logger_stream = get_logger()

    ctx = Context(
        config=config,
        fs=osfs.OSFS(config.get('etl', 'files_dir'), create=True,
                     create_mode=constants.DIR_PERMS),
        engine=create_engine(config['db'], echo=args.verbose),
        report=Report(logger, logger_stream),
        mode=args.mode
    )

    if args.command == 'etl':
        etl(args.processes, args.start, args.end, ctx)
    elif args.command == 'console':
        console(ctx)
    elif args.command == 'info':
        info(ctx)
    else:
        raise RuntimeError('Invalid command.')


main()
