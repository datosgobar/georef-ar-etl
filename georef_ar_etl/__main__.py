import logging
import configparser
import sqlalchemy
from fs import osfs
from .context import Context
from . import models

from . import provinces, departments, municipalities, localities, streets


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


def main():
    config = configparser.ConfigParser()
    config.read('config/georef.cfg')

    engine = sqlalchemy.create_engine(
        'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(
            **config['db']), echo=True)

    models.Base.metadata.create_all(engine)

    context = Context(
        config=config,
        filesystem=osfs.OSFS(config.get('etl', 'cache_dir'), create=True,
                             create_mode=0o700),
        engine=engine,
        logger=get_logger()
    )

    processes = [
        provinces.ProvincesETL(),
        departments.DepartmentsETL(),
        municipalities.MunicipalitiesETL(),
        localities.LocalitiesETL(),
        streets.StreetsETL()
    ]

    for process in processes:
        process.run(context)


main()
