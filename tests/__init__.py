import os
import logging
from unittest import TestCase
from fs import tempfs
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from georef_ar_etl.context import Context
from georef_ar_etl.loaders import Ogr2ogrStep
from georef_ar_etl.models import Base
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
            engine=create_engine(config['test_db']),
            logger=logger,
            mode='testing'
        )

    @classmethod
    def tearDownClass(cls):
        Base.metadata.drop_all(cls._ctx.engine)

    def tearDown(self):
        self._ctx.session.commit()
        self._metadata.drop_all(self._ctx.engine)
        self._metadata.clear()
        self._ctx.fs.clean()

    @classmethod
    def create_table(cls, name, columns_data, pkey):
        columns = [
            Column(col_name, col_type, primary_key=(col_name == pkey))
            for col_name, col_type in columns_data.items()
        ]

        table = Table(name, cls._metadata, *columns)
        table.create(bind=cls._ctx.engine)
        base = automap_base(metadata=cls._metadata)
        base.prepare()

        return getattr(base.classes, name)

    @classmethod
    def create_test_provinces(cls):
        cls.copy_test_file('test_provincias/test_provincias.dbf')
        cls.copy_test_file('test_provincias/test_provincias.shp')
        cls.copy_test_file('test_provincias/test_provincias.shx')
        cls.copy_test_file('test_provincias/test_provincias.prj')

        loader = Ogr2ogrStep(table_name='tmp_provincias',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        return loader.run('test_provincias', cls._ctx)

    @classmethod
    def copy_test_file(cls, filepath):
        dirname = os.path.dirname(filepath)
        if dirname:
            cls._ctx.fs.makedirs(dirname, permissions=constants.DIR_PERMS,
                                 recreate=True)

        with open(os.path.join(TEST_FILES_DIR, filepath), 'rb') as f:
            cls._ctx.fs.upload(filepath, f)
