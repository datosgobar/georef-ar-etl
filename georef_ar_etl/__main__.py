import logging
import configparser
from fs import osfs
from .components.context import Context
from . import territories


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

    # pipeline = territories.provinces_pipeline()
    # result = pipeline.run(
    #     'http://www.ign.gob.ar/descargas/geodatos/provincia.zip',
    #     context
    # )

    # print(result)

    # pipeline = territories.departments_pipeline()
    # result = pipeline.run(
    #     'http://www.ign.gob.ar/descargas/geodatos/departamento.zip',
    #     context
    # )

    # print(result)

    # pipeline = territories.municipalities_pipeline()
    # result = pipeline.run(
    #     'http://www.ign.gob.ar/descargas/geodatos/municipio.zip',
    #     context
    # )

    # print(result)

    pipeline = territories.localities_pipeline()
    result = pipeline.run(
        'http://www.bahra.gob.ar/descargas/BAHRA_2014_version_1.1_shape.tar.gz',
        context
    )

    print(result)


main()
