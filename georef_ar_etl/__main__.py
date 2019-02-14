import argparse
import logging
import configparser
import code
import sqlalchemy
from fs import osfs
from .context import Context, RUN_MODES
from . import models, constants

from . import provinces, departments, municipalities, localities, streets
from . import countries

DATA_PATH = 'data'
CONFIG_PATH = 'config/georef.cfg'
PROCESSES = [
    constants.PROVINCES,
    constants.DEPARTMENTS,
    constants.MUNICIPALITIES,
    constants.LOCALITIES,
    constants.STREETS
]


def get_logger():
    logger = logging.getLogger('georef-ar-etl')
    logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('{asctime} - {levelname:^7s} - {message}',
                                  '%Y-%m-%d %H:%M:%S', style='{')
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    return logger


def create_engine(config, echo=False):
    engine = sqlalchemy.create_engine(
            'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(
                **config['db']), echo=echo)

    models.Base.metadata.create_all(engine)
    return engine


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processes', action='append',
                        choices=PROCESSES)
    parser.add_argument('-m', '--mode', choices=RUN_MODES, default='normal')
    parser.add_argument('-c', '--console', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')

    return parser.parse_args()


def etl(enabled_processes, ctx):
    processes = [
        countries.CountriesETL(),
        provinces.ProvincesETL(),
        departments.DepartmentsETL(),
        municipalities.MunicipalitiesETL(),
        localities.LocalitiesETL(),
        streets.StreetsETL()
    ]

    for process in processes:
        if not enabled_processes or process.name in enabled_processes:
            process.run(ctx)


def console(ctx):
    console = code.InteractiveConsole(locals=locals())
    console.push('from georef_ar_etl import models')
    console.interact()


def main():
    args = parse_args()
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    ctx = Context(
        config=config,
        data_fs=osfs.OSFS(DATA_PATH),
        cache_fs=osfs.OSFS(config.get('etl', 'cache_dir'), create=True,
                           create_mode=0o700),
        engine=create_engine(config, echo=args.verbose),
        logger=get_logger(),
        mode=args.mode
    )

    if args.console:
        console(ctx)
    else:
        etl(ctx)


main()
