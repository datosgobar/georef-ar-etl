import logging
import configparser
from fs import osfs
from .components.context import Context
from . import processes


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

    context = Context(
        config=config,
        filesystem=osfs.OSFS('cache', create=True, create_mode=0o700),
        logger=get_logger()
    )

    pipeline = processes.provinces_pipeline()
    pipeline.run(config['etl']['provinces_url'], context)

    pipeline = processes.departments_pipeline()
    pipeline.run(config['etl']['departments_url'], context)

    pipeline = processes.municipalities_pipeline()
    pipeline.run(config['etl']['municipalities_url'], context)

    pipeline = processes.localities_pipeline()
    pipeline.run(config['etl']['localities_url'], context)

    pipeline = processes.streets_pipeline()
    pipeline.run(config['etl']['streets_url'], context)


main()
