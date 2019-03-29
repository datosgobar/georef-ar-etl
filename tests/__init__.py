import os
import logging
from unittest import TestCase
from fs import tempfs
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from georef_ar_etl import get_logger
from georef_ar_etl.context import Context, Report
from georef_ar_etl.loaders import Ogr2ogrStep
from georef_ar_etl.provinces import ProvincesExtractionStep
from georef_ar_etl.departments import DepartmentsExtractionStep
from georef_ar_etl.municipalities import MunicipalitiesExtractionStep
from georef_ar_etl.streets import StreetsExtractionStep
from georef_ar_etl.utils import CopyFileStep
from georef_ar_etl import read_config, create_engine, models

TEST_FILES_DIR = 'tests/test_files'


class ETLTestCase(TestCase):
    _uses_db = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tmp_provinces = None
        self._tmp_departments = None
        self._tmp_municipalities = None
        self._tmp_localities = None
        self._tmp_streets = None

    @classmethod
    def setUpClass(cls):
        if 'VERBOSE' in os.environ:
            logger, _ = get_logger()
        else:
            logger = logging.getLogger('georef-ar-etl')
            logger.addHandler(logging.NullHandler())

        config = read_config()
        cls._metadata = MetaData()
        cls._ctx = Context(
            config=config,
            fs=tempfs.TempFS(),
            engine=create_engine(config['test_db'], init_models=cls._uses_db),
            report=Report(logger),
            mode='testing'
        )

    @classmethod
    def tearDownClass(cls):
        if cls._uses_db:
            cls._ctx.session.commit()
            models.Base.metadata.drop_all(cls._ctx.engine)

    def tearDown(self):
        if self._uses_db:
            self._ctx.session.commit()
            self._metadata.drop_all(self._ctx.engine)
            self._metadata.clear()

        self._ctx.fs.removetree('.')
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
    def create_test_provinces(cls, extract=False):
        # Cargar la provincia de prueba
        cls.copy_test_file('test_provincias/test_provincias.dbf')
        cls.copy_test_file('test_provincias/test_provincias.shp')
        cls.copy_test_file('test_provincias/test_provincias.shx')
        cls.copy_test_file('test_provincias/test_provincias.prj')

        loader = Ogr2ogrStep(table_name='tmp_provincias',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_provinces = loader.run('test_provincias', cls._ctx)

        if extract:
            step = ProvincesExtractionStep()
            step.run(cls._tmp_provinces, cls._ctx)

        return cls._tmp_provinces

    @classmethod
    def create_test_departments(cls, extract=False):
        # Cargar los departamentos de la provincia de prueba
        cls.copy_test_file('test_departamentos/test_departamentos.dbf')
        cls.copy_test_file('test_departamentos/test_departamentos.shp')
        cls.copy_test_file('test_departamentos/test_departamentos.shx')
        cls.copy_test_file('test_departamentos/test_departamentos.prj')

        loader = Ogr2ogrStep(table_name='tmp_departamentos',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_departments = loader.run('test_departamentos', cls._ctx)

        if extract:
            step = DepartmentsExtractionStep()
            step.run(cls._tmp_departments, cls._ctx)

        return cls._tmp_departments

    @classmethod
    def create_test_municipalities(cls, extract=False):
        # Cargar los municipios de la provincia de prueba
        cls.copy_test_file('test_municipios/test_municipios.dbf')
        cls.copy_test_file('test_municipios/test_municipios.shp')
        cls.copy_test_file('test_municipios/test_municipios.shx')
        cls.copy_test_file('test_municipios/test_municipios.prj')

        loader = Ogr2ogrStep(table_name='tmp_municipios',
                             geom_type='MultiPolygon', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_municipalities = loader.run('test_municipios', cls._ctx)

        if extract:
            step = MunicipalitiesExtractionStep()
            step.run(cls._tmp_municipalities, cls._ctx)

        return cls._tmp_municipalities

    @classmethod
    def create_test_localities(cls):
        # Cargar las localidades de la provincia de prueba
        cls.copy_test_file('test_localidades/test_localidades.dbf')
        cls.copy_test_file('test_localidades/test_localidades.shp')
        cls.copy_test_file('test_localidades/test_localidades.shx')
        cls.copy_test_file('test_localidades/test_localidades.prj')

        loader = Ogr2ogrStep(table_name='tmp_localidades',
                             geom_type='MultiPoint', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_localities = loader.run('test_localidades', cls._ctx)
        return cls._tmp_localities

    @classmethod
    def create_test_streets(cls, extract=False):
        # Cargar las calles de la provincia de prueba
        cls.copy_test_file('test_calles/test_calles.dbf')
        cls.copy_test_file('test_calles/test_calles.shp')
        cls.copy_test_file('test_calles/test_calles.shx')
        cls.copy_test_file('test_calles/test_calles.prj')

        loader = Ogr2ogrStep(table_name='tmp_calles',
                             geom_type='MultiLineString', encoding='utf-8',
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_streets = loader.run('test_calles', cls._ctx)

        if extract:
            step = StreetsExtractionStep()
            step.run(cls._tmp_streets, cls._ctx)

        return cls._tmp_streets

    @classmethod
    def copy_test_file(cls, filepath):
        # Copiar archivo desde directorio tests/test_files/ (OSFS) al
        # directorio del test actual (TempFS).
        abspath = os.path.abspath(os.path.join(TEST_FILES_DIR, filepath))
        CopyFileStep(filepath).run(abspath, cls._ctx)
