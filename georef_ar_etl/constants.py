ETL_VERSION = '9.0.0'
DATA_DIR = 'data'
LATEST_DIR = 'latest'
CONFIG_PATH = 'config/georef.cfg'
TEST_CONFIG_PATH = 'config/georef_test.cfg'
DIR_PERMS = 0o700

BAHRA_TYPES = {
    'E': 'Entidad (E)',
    'LC': 'Componente de localidad compuesta (LC)',
    'LS': 'Localidad simple (LS)'
}

COUNTRIES = 'paises'
PROVINCES = 'provincias'
DEPARTMENTS = 'departamentos'
MUNICIPALITIES = 'municipios'
LOCALITIES = 'localidades'
STREETS = 'calles'
INTERSECTIONS = 'intersecciones'

TMP_TABLE_NAME = 'tmp_{}'
ETL_TABLE_NAME = 'georef_{}'

COUNTRIES_ETL_TABLE = ETL_TABLE_NAME.format(COUNTRIES)
PROVINCES_ETL_TABLE = ETL_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_ETL_TABLE = ETL_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_ETL_TABLE = ETL_TABLE_NAME.format(MUNICIPALITIES)
LOCALITIES_ETL_TABLE = ETL_TABLE_NAME.format(LOCALITIES)
STREETS_ETL_TABLE = ETL_TABLE_NAME.format(STREETS)
INTERSECTIONS_ETL_TABLE = ETL_TABLE_NAME.format(INTERSECTIONS)

COUNTRIES_TMP_TABLE = TMP_TABLE_NAME.format(COUNTRIES)
PROVINCES_TMP_TABLE = TMP_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_TMP_TABLE = TMP_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_TMP_TABLE = TMP_TABLE_NAME.format(MUNICIPALITIES)
LOCALITIES_TMP_TABLE = TMP_TABLE_NAME.format(LOCALITIES)
STREETS_TMP_TABLE = TMP_TABLE_NAME.format(STREETS)

PROVINCE_ID_LEN = 2
DEPARTMENT_ID_LEN = 5
MUNICIPALITY_ID_LEN = 6
LOCALITY_ID_LEN = 11
STREET_ID_LEN = 13

STREETS_SOURCE = 'INDEC'
