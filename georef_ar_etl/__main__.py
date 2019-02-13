import logging
import configparser
import sqlalchemy
from fs import osfs
from .context import Context
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

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    return logger


def create_engine(config):
    engine = sqlalchemy.create_engine(
            'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(
                **config['db']))

    models.Base.metadata.create_all(engine)
    return engine


def main():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    context = Context(
        config=config,
        data_fs=osfs.OSFS(DATA_PATH),
        cache_fs=osfs.OSFS(config.get('etl', 'cache_dir'), create=True,
                           create_mode=0o700),
        engine=create_engine(config),
        logger=get_logger(),
        interactive=True
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
        process.run(context)


main()
