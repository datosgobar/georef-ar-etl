ETL_VERSION = '9.0.0'
DATA_DIR = 'data'
LATEST_DIR = 'latest'
CONFIG_PATH = 'config/georef.cfg'
DIR_PERMS = 0o700

BAHRA_TYPES = {
    'E': 'Entidad (E)',
    'LC': 'Componente de localidad compuesta (LC)',
    'LS': 'Localidad simple (LS)'
}

# Existen dos numeraciones posibles para las comunas de CABA:
# Del 1 al 15,
# O del 7 al 105 (1 * 7 al 15 * 7)
# La numeración preferida es la del 1 al 15, por lo que los datos
# que usan la numeración alterna deben ser modificados para usar
# la otra. Para lograr esto, se divide por 7 el ID de la comuna.
CABA_DIV_FACTOR = 7

PROVINCES = 'provincias'
DEPARTMENTS = 'departamentos'
MUNICIPALITIES = 'municipios'
LOCALITIES = 'localidades'
STREETS = 'calles'
INTERSECTIONS = 'intersecciones'

TMP_TABLE_NAME = 'tmp_{}'
ETL_TABLE_NAME = 'georef_{}'

PROVINCES_ETL_TABLE = ETL_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_ETL_TABLE = ETL_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_ETL_TABLE = ETL_TABLE_NAME.format(MUNICIPALITIES)
LOCALITIES_ETL_TABLE = ETL_TABLE_NAME.format(LOCALITIES)
STREETS_ETL_TABLE = ETL_TABLE_NAME.format(STREETS)
INTERSECTIONS_ETL_TABLE = ETL_TABLE_NAME.format(INTERSECTIONS)

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
