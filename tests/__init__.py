import os
import logging
from fs import tempfs
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from unittest import TestCase
from georef_ar_etl.context import Context
from georef_ar_etl import read_config, create_engine, constants

TEST_FILES_DIR = 'tests/test_files'


class ETLTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger('georef-ar-etl')
        config = read_config()
        cls._metadata = MetaData()
        cls._ctx = Context(
            config=config,
            fs=tempfs.TempFS(),
            engine=create_engine(config),
            logger=logger,
            mode='testing'
        )

    def tearDown(self):
        self._ctx.session.commit()
        self._metadata.drop_all(self._ctx.engine)
        self._metadata.clear()
        self._ctx.fs.clean()

    def create_table(self, name, columns_data, pkey=None):
        if not pkey:
            pkey = columns_data[0][0]

        columns = [
            Column(*column_data, primary_key=(column_data[0] == pkey))
            for column_data in columns_data
        ]

        table = Table(name, self._metadata, *columns)
        table.create(bind=self._ctx.engine)
        base = automap_base(metadata=self._metadata)
        base.prepare()

        return getattr(base.classes, name)

    def copy_test_file(self, filepath):
        dirname = os.path.dirname(filepath)
        if dirname:
            self._ctx.fs.makedirs(dirname, permissions=constants.DIR_PERMS,
                                  recreate=True)

        with open(os.path.join(TEST_FILES_DIR, filepath), 'rb') as f:
            self._ctx.fs.upload(filepath, f)
