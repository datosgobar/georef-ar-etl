import sys
import csv
from tqdm import tqdm


def clean_string(s):
    s = s.splitlines()[0]
    return s.strip()


def pbar(iterator, ctx, total=None):
    if not ctx.interactive:
        yield from iterator
        return

    yield from tqdm(iterator, file=sys.stderr, total=total)


def load_data_csv(filename, ctx):
    with ctx.data.open(filename, newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)
