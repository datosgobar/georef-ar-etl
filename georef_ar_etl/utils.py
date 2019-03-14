import os
import sys
import csv
from sqlalchemy.sql import sqltypes
from geoalchemy2 import types as geotypes
from tqdm import tqdm
from .exceptions import ProcessException
from .process import Step
from . import constants

_SQL_TYPES = {
    'varchar': sqltypes.VARCHAR,
    'integer': sqltypes.INTEGER,
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
        for name, col_type in table.__table__.columns:
            if name not in self._schema:
                raise ProcessException(
                    'La columna "{}" no está presente en el esquema'.format(
                        name))

            col_class = _SQL_TYPES[self._schema[name]]
            if not isinstance(col_type, col_class):
                raise ProcessException('')

        return table


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
