import configparser
import logging
import sqlalchemy
from . import models, constants


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


def read_config():
    config = configparser.ConfigParser()
    config.read(constants.CONFIG_PATH)
    return config
