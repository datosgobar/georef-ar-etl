import argparse
import logging
import configparser
import sqlalchemy
from fs import osfs
from .context import Context, RUN_MODES
from . import models

from . import provinces, departments, municipalities, localities, streets
from . import countries

DATA_PATH = 'data'
CONFIG_PATH = 'config/georef.cfg'


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


def create_engine(config):
    engine = sqlalchemy.create_engine(
            'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(
                **config['db']))

    models.Base.metadata.create_all(engine)
    return engine


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--procesos', action='append', dest='processes')
    parser.add_argument('-m', '--modo', dest='mode', choices=RUN_MODES,
                        default='normal')

    return parser.parse_args()


def main():
    args = parse_args()
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    context = Context(
        config=config,
        data_fs=osfs.OSFS(DATA_PATH),
        cache_fs=osfs.OSFS(config.get('etl', 'cache_dir'), create=True,
                           create_mode=0o700),
        engine=create_engine(config),
        logger=get_logger(),
        mode=args.mode
    )

    processes = [
        countries.CountriesETL(),
        provinces.ProvincesETL(),
        departments.DepartmentsETL(),
        municipalities.MunicipalitiesETL(),
        localities.LocalitiesETL(),
        streets.StreetsETL()
    ]

    for process in processes:
        if not args.processes or process.name in args.processes:
            process.run(context)


main()
