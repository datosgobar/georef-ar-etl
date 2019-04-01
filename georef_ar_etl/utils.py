import os
import sys
import csv
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
    'double': pgtypes.DOUBLE_PRECISION,
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
        for name, col_info in table.__table__.columns.items():
            if name not in self._schema:
                raise ProcessException(
                    'La columna "{}" no está presente en el esquema.'.format(
                        name))

            col_class = _SQL_TYPES[self._schema[name]]
            if not isinstance(col_info.type, col_class):
                raise ProcessException(
                    'La columna "{}" debería ser de tipo {}.'.format(
                        name, self._schema[name]))

        for name in self._schema:
            if name not in table.__table__.columns:
                raise ProcessException(
                    'La columna "{}" no está presente en la tabla.'.format(
                        name))

        return table


class ValidateTableSizeStep(Step):
    def __init__(self, size=None, tolerance=0):
        super().__init__('validate_table_size')
        self._size = size
        self._tolerance = tolerance

    def _run_internal(self, table, ctx):
        if ctx.mode == 'interactive':
            ctx.report.info('Salteando chequeo de tamaño.')
            return table

        count = ctx.session.query(table).count()
        diff = abs(self._size - count)

        if diff > self._tolerance:
            raise ProcessException(
                'La tabla contiene {} elementos, pero debe contar con {} '
                '(margen de error: {}).'.format(count, self._size,
                                                self._tolerance))
        elif diff > 0:
            ctx.report.info(
                'La cantidad de elementos es {} (esperado: {})'.format(
                    count, self._size))

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
        dirname = os.path.dirname(self._dst)
        if dirname:
            ensure_dir(dirname, ctx)

        if os.path.isabs(src):
            src_fs = fs.osfs.OSFS('/')
        else:
            src_fs = ctx.fs

        fs.copy.copy_file(
            dst_path=self._dst,
            dst_fs=ctx.fs,
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


def clean_string(s):
    s = s.splitlines()[0]
    return s.strip()


def pbar(iterator, ctx, total=None):
    if ctx.mode != 'interactive':
        yield from iterator
        return

    yield from tqdm(iterator, file=sys.stderr, total=total)


def ensure_dir(path, ctx):
    ctx.fs.makedirs(path, permissions=constants.DIR_PERMS, recreate=True)


def copy_file(src, dst, ctx):
    ctx.fs.copy(src, dst, overwrite=True)


def load_data_csv(filename):
    with open(os.path.join(constants.DATA_DIR, filename), newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)
