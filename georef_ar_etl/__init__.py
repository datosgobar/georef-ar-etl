from io import StringIO
import configparser
import logging
import sqlalchemy
from . import models, constants


def get_logger():
    logger = logging.getLogger('georef-ar-etl')
    logger.setLevel(logging.INFO)

    logger_stream = StringIO()
    str_handler = logging.StreamHandler(logger_stream)
    str_handler.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('{asctime} - {levelname:^7s} - {message}',
                                  '%Y-%m-%d %H:%M:%S', style='{')
    stdout_handler.setFormatter(formatter)
    str_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(str_handler)
    return logger, logger_stream


def create_engine(db_config, echo=False, init_models=True):
    engine = sqlalchemy.create_engine(
        'postgresql+psycopg2://{user}:{password}@{host}/{database}'.format(
            **db_config), echo=echo)

    if init_models:
        models.Base.metadata.create_all(engine)

    return engine


def read_config():
    config = configparser.ConfigParser()
    config.read(constants.CONFIG_PATH)
    return config
