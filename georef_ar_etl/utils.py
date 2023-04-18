"""Módulo 'utils' de georef-ar-etl.

Define funciones y clases de utilidades varias.

"""

import os
import sys
import csv
import operator
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects.postgresql import base as pgtypes
from geoalchemy2 import types as geotypes
import fs
from tqdm import tqdm
from .exceptions import ProcessException
from .process import Step
from . import constants

_SQL_TYPES = {
    'varchar': sqltypes.VARCHAR,
    'integer': sqltypes.INTEGER,
    'numeric': sqltypes.NUMERIC,
    'double': pgtypes.DOUBLE_PRECISION,
    'date': sqltypes.Date,
    'geometry': geotypes.Geometry
}


class CheckDependenciesStep(Step):
    def __init__(self, dependencies):
        super().__init__('check_dependencies', reads_input=False)
        self._dependencies = dependencies

    def _run_internal(self, data, ctx):
        for dep in self._dependencies:
            if isinstance(dep, str):
                dep = automap_table(dep, ctx)

            if not ctx.session.query(dep).first():
                raise ProcessException(
                    'La tabla "{}" está vacía.'.format(dep.__table__.name))


class DropTableStep(Step):
    def __init__(self):
        super().__init__('drop_table')

    def _run_internal(self, table, ctx):
        ctx.report.info('Eliminando tabla: "{}"'.format(table.__table__.name))
        # Asegurarse de ejecutar cualquier transacción pendiende primero
        ctx.session.commit()

        if ctx.mode == 'interactive':
            ctx.report.info('Salteando eliminado de tabla.')
        else:
            # Eliminar la tabla
            table.__table__.drop(ctx.engine)


class ValidateTableSchemaStep(Step):
    def __init__(self, schema):
        super().__init__('validate_table_schema')

        for value in schema.values():
            if value not in _SQL_TYPES:
                raise ValueError('Unknown type: {}.'.format(value))

        self._schema = schema

    def _run_internal(self, table, ctx):
        schema_field_missing = []
        table_field_missing = []
        type_field_wrong = []
        for name, col_info in table.__table__.columns.items():
            if name not in self._schema:
                schema_field_missing.append(name)
                continue

            col_class = _SQL_TYPES[self._schema[name]]
            if not isinstance(col_info.type, col_class):
                type_field_wrong.append(
                    'La columna "{}" debería ser de tipo {}. '.format(
                        name, col_info.type))

        for name in self._schema:
            if name not in table.__table__.columns:
                table_field_missing.append(name)

        msg = 'Las columnas "{}" no están presentes en el esquema. '.format(
            ",".join(schema_field_missing)
        ) if schema_field_missing else ''
        msg = msg + 'Las columnas "{}" no están presentes en la tabla. '.format(
            ",".join(table_field_missing)
        ) if table_field_missing else msg
        msg = msg + "No coinciden los tipos: {}".format(
            "".join(type_field_wrong)
        ) if type_field_wrong else msg

        if msg:
            raise ProcessException(msg)

        return table


class ValidateTableSizeStep(Step):
    def __init__(self, target_size, op='eq'):
        super().__init__('validate_table_size')
        self._target_size = target_size
        self._operator = getattr(operator, op)

    def _run_internal(self, table, ctx):
        count = ctx.session.query(table).count()
        ctx.report.info('Tamaño objetivo: {}'.format(self._target_size))
        ctx.report.info('Operador: "{}"'.format(self._operator.__name__))

        if not self._operator(count, self._target_size):
            message = ('La tabla contiene {} elementos, pero falló la '
                       'validación contra target_size={}, utilizando el '
                       'operador "{}".'.format(count, self._target_size,
                                               self._operator.__name__))

            if ctx.mode != 'interactive':
                raise ProcessException(message)

            # En modo interactivo, permitir que las tablas tomen tamaños no
            # esperados.
            ctx.report.warn(message)

        return table


class FunctionStep(Step):
    def __init__(self, *args, fn=None, ctx_fn=None, name=None, **kwargs):
        super().__init__(name or 'apply_function', *args, **kwargs)
        if fn and ctx_fn:
            raise RuntimeError('Only fn or ctx_fn must be defined')

        self._fn = fn
        self._ctx_fn = ctx_fn

    def _run_internal(self, data, ctx):
        return self._fn(data) if self._fn else self._ctx_fn(data, ctx)


FirstResultStep = FunctionStep(fn=lambda results: results[0],
                               name='first_result')


class CopyFileStep(Step):
    def __init__(self, *dst_parts):
        super().__init__('copy_file')
        self._dst = os.path.join(*dst_parts)

    def _run_internal(self, src, ctx):
        if os.path.isabs(self._dst):
            dst_fs = fs.osfs.OSFS('/')
        else:
            dst_fs = ctx.fs

        dirname = os.path.dirname(self._dst)
        if dirname:
            ensure_dir(dirname, dst_fs)

        if os.path.isabs(src):
            src_fs = fs.osfs.OSFS('/')
        else:
            src_fs = ctx.fs

        ctx.report.info('Copiando desde:')
        ctx.report.info('-> {}'.format(src))
        ctx.report.info('A:')
        ctx.report.info('-> {}'.format(self._dst))

        fs.copy.copy_file(
            dst_path=self._dst,
            dst_fs=dst_fs,
            src_path=src,
            src_fs=src_fs
        )

        return self._dst


def automap_table(table_name, ctx, metadata=None):
    if not metadata:
        metadata = MetaData()

    metadata.reflect(ctx.engine, only=[table_name])
    base = automap_base(metadata=metadata)
    base.prepare()

    return getattr(base.classes, table_name)


def update_entity(new_entity, prev_entity):
    for attribute, val in vars(new_entity).items():
        if not attribute.startswith('_'):
            setattr(prev_entity, attribute, val)


def clean_string(s):
    # Si hay más de una línea, tomar la primera
    s = s.splitlines()[0]
    # Normalizar espacios
    s = ' '.join(s.split())
    return s


def add_maybe_flush(obj, ctx, bulk_size=None):
    bulk_size = bulk_size or ctx.config.getint('etl', 'bulk_size')

    ctx.session.add(obj)
    if len(ctx.session.new) >= bulk_size:
        ctx.session.flush()
        ctx.session.expunge_all()


def pbar(iterator, ctx, total=None):
    if ctx.mode == 'testing':
        yield from iterator
        return

    yield from tqdm(iterator, file=sys.stderr, total=total)


def ensure_dir(path, filesystem):
    filesystem.makedirs(path, permissions=constants.DIR_PERMS, recreate=True)


def load_data_csv(filename):
    with open(os.path.join(constants.DATA_DIR, filename), newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)
