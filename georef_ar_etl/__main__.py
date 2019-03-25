import argparse
import code
from fs import osfs
from .context import Context, RUN_MODES
from . import read_config, get_logger, create_engine, constants
from . import provinces, departments, municipalities, localities
from . import streets, intersections

PROCESSES = [
    constants.PROVINCES,
    constants.DEPARTMENTS,
    constants.MUNICIPALITIES,
    constants.LOCALITIES,
    constants.STREETS,
    constants.INTERSECTIONS
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
                        help='Modo de ejecución.')
    parser.add_argument('-s', '--start', default=1, type=int,
                        help='Paso desde el cual comenzar (número).')
    parser.add_argument('-c', '--console', action='store_true',
                        help='Iniciar una consola interactiva.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Imprimir información adicional.')

    return parser.parse_args()


def etl(enabled_processes, start, ctx):
    modules = [
        provinces,
        departments,
        municipalities,
        localities,
        streets,
        intersections
    ]

    processes = [module.create_process(ctx.config) for module in modules]

    for process in processes:
        if not enabled_processes or process.name in enabled_processes:
            process.run(start - 1, ctx)


# pylint: disable=unused-argument
def console(ctx):
    repl = code.InteractiveConsole(locals=locals())
    repl.push('from georef_ar_etl import models')
    repl.interact()


def main():
    args = parse_args()
    config = read_config()

    ctx = Context(
        config=config,
        fs=osfs.OSFS(config.get('etl', 'files_dir'), create=True,
                     create_mode=constants.DIR_PERMS),
        engine=create_engine(config['db'], echo=args.verbose),
        logger=get_logger(),
        mode=args.mode
    )

    if args.console:
        console(ctx)
    else:
        etl(args.processes, args.start, ctx)


main()
