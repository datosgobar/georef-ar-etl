import os
import logging
from unittest import TestCase
from fs import tempfs
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from georef_ar_etl import get_logger
from georef_ar_etl.context import Context
from georef_ar_etl.loaders import Ogr2ogrStep
from georef_ar_etl import read_config, create_engine, constants, models

TEST_FILES_DIR = 'tests/test_files'


class ETLTestCase(TestCase):
    _uses_db = True

    @classmethod
    def setUpClass(cls):
        if 'VERBOSE' in os.environ:
            logger = get_logger()
        else:
            logger = logging.getLogger('georef-ar-etl')
            logger.addHandler(logging.NullHandler())

        config = read_config()
        cls._metadata = MetaData()
        cls._ctx = Context(
            config=config,
            fs=tempfs.TempFS(),
            engine=create_engine(config['test_db'], init_models=cls._uses_db),
            logger=logger,
            mode='testing'
        )

    @classmethod
    def tearDownClass(cls):
        if cls._uses_db:
            models.Base.metadata.drop_all(cls._ctx.engine)

    def tearDown(self):
        if self._uses_db:
            for table in models.Base.metadata.tables.values():
                result = self._ctx.engine.execute(table.delete())
                result.close()

            self._ctx.session.commit()
            self._metadata.drop_all(self._ctx.engine)
            self._metadata.clear()

        self._ctx.fs.clean()
        self._ctx.report.reset()

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
        # Cargar la provincia de Santa Fe
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
    def create_test_departments(cls):
        # Cargar los departamentos de la provincia de Santa Fe
        cls.copy_test_file('test_departamentos/test_departamentos.dbf')
        cls.copy_test_file('test_departamentos/test_departamentos.shp')
        cls.copy_test_file('test_departamentos/test_departamentos.shx')
        cls.copy_test_file('test_departamentos/test_departamentos.prj')

        loader = Ogr2ogrStep(table_name='tmp_departamentos',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        return loader.run('test_departamentos', cls._ctx)

    @classmethod
    def create_test_municipalities(cls):
        # Cargar los municipios de la provincia de Santa Fe
        cls.copy_test_file('test_municipios/test_municipios.dbf')
        cls.copy_test_file('test_municipios/test_municipios.shp')
        cls.copy_test_file('test_municipios/test_municipios.shx')
        cls.copy_test_file('test_municipios/test_municipios.prj')

        loader = Ogr2ogrStep(table_name='tmp_municipios',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        return loader.run('test_municipios', cls._ctx)

    @classmethod
    def copy_test_file(cls, filepath):
        dirname = os.path.dirname(filepath)
        if dirname:
            cls._ctx.fs.makedirs(dirname, permissions=constants.DIR_PERMS,
                                 recreate=True)

        with open(os.path.join(TEST_FILES_DIR, filepath), 'rb') as f:
            cls._ctx.fs.upload(filepath, f)
