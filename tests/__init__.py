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
from georef_ar_etl.local_governments import LocalGovernmentsExtractionStep
from georef_ar_etl.census_localities import CensusLocalitiesExtractionStep
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
        self._tmp_local_governments = None
        self._tmp_settlements = None
        self._tmp_census_localities = None
        self._tmp_blocks = None

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
                             geom_type='MultiPolygon',
                             env={'SHAPE_ENCODING': 'utf-8'},
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
                             geom_type='MultiPolygon',
                             env={'SHAPE_ENCODING': 'utf-8'},
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_departments = loader.run('test_departamentos', cls._ctx)

        if extract:
            step = DepartmentsExtractionStep()
            step.run(cls._tmp_departments, cls._ctx)

        return cls._tmp_departments

    @classmethod
    def create_test_local_governments(cls, extract=False):
        # Cargar los gobiernos locales de la provincia de prueba
        cls.copy_test_file('test_gobiernos_locales/test_gobiernos_locales.dbf')
        cls.copy_test_file('test_gobiernos_locales/test_gobiernos_locales.shp')
        cls.copy_test_file('test_gobiernos_locales/test_gobiernos_locales.shx')
        cls.copy_test_file('test_gobiernos_locales/test_gobiernos_locales.prj')

        loader = Ogr2ogrStep(table_name='tmp_gobiernos_locales',
                             geom_type='MultiPolygon',
                             env={'SHAPE_ENCODING': 'utf-8'},
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_local_governments = loader.run('test_gobiernos_locales', cls._ctx)

        if extract:
            step = LocalGovernmentsExtractionStep()
            step.run(cls._tmp_local_governments, cls._ctx)

        return cls._tmp_local_governments

    @classmethod
    def create_test_settlements(cls):
        # Cargar los asentamientos de la provincia de prueba
        cls.copy_test_file('test_asentamientos/test_asentamientos.dbf')
        cls.copy_test_file('test_asentamientos/test_asentamientos.shp')
        cls.copy_test_file('test_asentamientos/test_asentamientos.shx')
        cls.copy_test_file('test_asentamientos/test_asentamientos.prj')

        loader = Ogr2ogrStep(table_name='tmp_asentamientos',
                             geom_type='MultiPoint',
                             env={'SHAPE_ENCODING': 'utf-8'},
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_settlements = loader.run('test_asentamientos', cls._ctx)
        return cls._tmp_settlements

    @classmethod
    def create_test_census_localities(cls, extract=False):
        # Cargar las localidades censales de la provincia de prueba
        cls.copy_test_file(
            'test_localidades_censales/test_localidades_censales.dbf')
        cls.copy_test_file(
            'test_localidades_censales/test_localidades_censales.shp')
        cls.copy_test_file(
            'test_localidades_censales/test_localidades_censales.shx')
        cls.copy_test_file(
            'test_localidades_censales/test_localidades_censales.prj')

        loader = Ogr2ogrStep(table_name='tmp_localidades_censales',
                             geom_type='Point',
                             env={'SHAPE_ENCODING': 'utf-8'},
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_census_localities = loader.run('test_localidades_censales',
                                                cls._ctx)

        if extract:
            step = CensusLocalitiesExtractionStep()
            step.run(cls._tmp_census_localities, cls._ctx)

        return cls._tmp_census_localities

    @classmethod
    def extract_streets(cls, blocks):
        step = StreetsExtractionStep()
        step.run((blocks, None), cls._ctx)

    @classmethod
    def create_test_blocks(cls):
        # Cargar las cuadras de la provincia de prueba
        cls.copy_test_file('test_cuadras/test_cuadras.dbf')
        cls.copy_test_file('test_cuadras/test_cuadras.shp')
        cls.copy_test_file('test_cuadras/test_cuadras.shx')
        cls.copy_test_file('test_cuadras/test_cuadras.prj')

        loader = Ogr2ogrStep(table_name='tmp_cuadras',
                             geom_type='MultiLineString',
                             env={'SHAPE_ENCODING': 'utf-8'},
                             metadata=cls._metadata,
                             db_config=cls._ctx.config['test_db'])

        cls._tmp_blocks = loader.run('test_cuadras', cls._ctx)
        return cls._tmp_blocks

    @classmethod
    def copy_test_file(cls, filepath):
        # Copiar archivo desde directorio tests/test_files/ (OSFS) al
        # directorio del test actual (TempFS).
        abspath = os.path.abspath(os.path.join(TEST_FILES_DIR, filepath))
        CopyFileStep(filepath).run(abspath, cls._ctx)
