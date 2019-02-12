from functools import partial
from .components import pipeline, extractors, transformers


def provinces_pipeline():
    return pipeline.Pipeline([
        partial(extractors.extract_url, 'provincia.zip'),
        transformers.transform_zipfile,
        partial(transformers.transform_ogr2ogr, 'MULTIPOLYGON', True)
    ])


def departments_pipeline():
    return pipeline.Pipeline([
        partial(extractors.extract_url, 'departamento.zip'),
        transformers.transform_zipfile,
        partial(transformers.transform_ogr2ogr, 'MULTIPOLYGON', True)
    ])


def municipalities_pipeline():
    return pipeline.Pipeline([
        partial(extractors.extract_url, 'municipio.zip'),
        transformers.transform_zipfile,
        partial(transformers.transform_ogr2ogr, 'MULTIPOLYGON', True)
    ])


def localities_pipeline():
    return pipeline.Pipeline([
        partial(extractors.extract_url, 'bahra.tar.gz'),
        transformers.transform_tarfile,
        partial(transformers.transform_ogr2ogr, 'MULTIPOINT', False)
    ])
