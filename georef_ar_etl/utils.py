import sys
import csv
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from tqdm import tqdm


def clean_string(s):
    s = s.splitlines()[0]
    return s.strip()


def pbar(iterator, ctx, total=None):
    if ctx.mode != 'interactive':
        yield from iterator
        return

    yield from tqdm(iterator, file=sys.stderr, total=total)


def load_data_csv(filename, ctx):
    with ctx.data.open(filename, newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)


def automap_table(table_name, ctx):
    metadata = MetaData()
    metadata.reflect(ctx.engine, only=[table_name])
    base = automap_base(metadata=metadata)
    base.prepare()

    return getattr(base.classes, table_name)
