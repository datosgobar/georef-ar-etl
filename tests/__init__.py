import logging
from fs import tempfs
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from unittest import TestCase
from georef_ar_etl.context import Context
from georef_ar_etl import read_config, create_engine


class ETLTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger('georef-ar-etl')
        config = read_config()
        cls._metadata = MetaData()
        cls._ctx = Context(
            config=config,
            fs=tempfs.TempFS(),
            engine=create_engine(config, echo=True),
            logger=logger,
            mode='testing'
        )

    @classmethod
    def tearDownClass(cls):
        cls._ctx.session.commit()
        cls._metadata.drop_all(cls._ctx.engine)
        cls._ctx.fs.clean()

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
