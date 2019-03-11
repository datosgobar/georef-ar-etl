import os
import sys
import csv
from tqdm import tqdm
from .process import Step, ProcessException
from . import constants


class CheckDependenciesStep(Step):
    def __init__(self, dependencies):
        super().__init__('check_dependencies')
        self._dependencies = dependencies

    def _run_internal(self, data, ctx):
        for dep in self._dependencies:
            if not ctx.query(dep).first():
                raise ProcessException(
                    'La tabla "{}" está vacía.'.format(dep.__table__.name))


class DropTableStep(Step):
    def __init__(self):
        super().__init__('drop_table')

    def _run_internal(self, table, ctx):
        ctx.logger.info('Eliminando tabla: "{}"'.format(table.__table__.name))
        # Asegurarse de ejecutar cualquier transacción pendiende primero
        ctx.session.commit()
        # Eliminar la tabla
        table.__table__.drop(ctx.engine)


class FunctionStep(Step):
    def __init__(self, fn):
        super().__init__('function')
        self._fn = fn

    def _run_internal(self, data, ctx):
        ctx.logger.info('Ejecutando función.')
        return self._fn(data)


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
