import os
import sys
import csv
from sqlalchemy.sql import sqltypes
from sqlalchemy.dialects.postgresql import base as pgtypes
from geoalchemy2 import types as geotypes
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
        # Eliminar la tabla
        table.__table__.drop(ctx.engine)


class ValidateTableSchemaStep(Step):
    def __init__(self, schema):
        super().__init__('validate_table_schema')

        for value in schema.values():
            if value not in _SQL_TYPES:
                raise ValueError('Unknown type: {}'.format(value))

        self._schema = schema

    def _run_internal(self, table, ctx):
        for name, col_info in table.__table__.columns.items():
            if name not in self._schema:
                raise ProcessException(
                    'La columna "{}" no está presente en el esquema'.format(
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
    def __init__(self, fn):
        super().__init__('apply_function')
        self._fn = fn

    def _run_internal(self, data, ctx):
        return self._fn(data)


FirstResultStep = FunctionStep(lambda xs: xs[0])


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
