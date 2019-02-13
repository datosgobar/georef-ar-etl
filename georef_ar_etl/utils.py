import sys
import csv
from tqdm import tqdm


def clean_string(s):
    s = s.splitlines()[0]
    return s.strip()


def pbar(iterator, context, total=None):
    if not context.interactive:
        yield from iterator
        return

    yield from tqdm(iterator, file=sys.stderr, total=total)


def load_data_csv(filename, context):
    with context.data.open(filename, newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)
