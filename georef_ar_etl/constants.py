ETL_VERSION = '9.0.0'
DATA_DIR = 'data'
CONFIG_PATH = 'config/georef.cfg'
DIR_PERMS = 0o700

BAHRA_TYPES = {
    'E': 'Entidad (E)',
    'LC': 'Componente de localidad compuesta (LC)',
    'LS': 'Localidad simple (LS)'
}

STREET_TYPE_OTHER = 'OTRO'

PROVINCE_IDS = [
    '02', '06', '10', '14', '18', '22', '26', '30', '34', '38', '42', '46',
    '50', '54', '58', '62', '66', '70', '74', '78', '82', '86', '90', '94'
]

# Existen dos numeraciones posibles para las comunas de CABA:
# - Del 1 al 15,
# - Del 7 al 105 (1 * 7 al 15 * 7)
# La numeración preferida es la del 1 al 15, por lo que los datos que usan la
# numeración alterna deben ser modificados para usar la otra. Para lograr esto,
# se divide por 7 el ID de la comuna.
CABA_DIV_FACTOR = 7

# Algunas entidades geográficamente dentro de CABA tienen IDs que comienzan con
# '02000', haciendo referencia a un departamento '000' de la provincia '02'
# (CABA). Como este departamento no existe, se acepta este ID de departamento
# como válido cuando se crean entidades, pero en la relación (modelo y DB) se
# almacena NULL como departamento.
CABA_VIRTUAL_DEPARTMENT_ID = '02000'

PROVINCES = 'provincias'
DEPARTMENTS = 'departamentos'
MUNICIPALITIES = 'municipios'
LOCALITIES = 'localidades'
STREETS = 'calles'
INTERSECTIONS = 'intersecciones'
STREET_BLOCKS = 'cuadras'
SYNONYMS = 'sinonimos'
EXCLUDING_TERMS = 'terminos_excluyentes'

TMP_TABLE_NAME = 'tmp_{}'
ETL_TABLE_NAME = 'georef_{}'

PROVINCES_ETL_TABLE = ETL_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_ETL_TABLE = ETL_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_ETL_TABLE = ETL_TABLE_NAME.format(MUNICIPALITIES)
LOCALITIES_ETL_TABLE = ETL_TABLE_NAME.format(LOCALITIES)
STREETS_ETL_TABLE = ETL_TABLE_NAME.format(STREETS)
INTERSECTIONS_ETL_TABLE = ETL_TABLE_NAME.format(INTERSECTIONS)
STREET_BLOCKS_ETL_TABLE = ETL_TABLE_NAME.format(STREET_BLOCKS)

PROVINCES_TMP_TABLE = TMP_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_TMP_TABLE = TMP_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_TMP_TABLE = TMP_TABLE_NAME.format(MUNICIPALITIES)
LOCALITIES_TMP_TABLE = TMP_TABLE_NAME.format(LOCALITIES)
STREETS_TMP_TABLE = TMP_TABLE_NAME.format(STREETS)
STREET_BLOCKS_TMP_TABLE = TMP_TABLE_NAME.format(STREET_BLOCKS)

PROVINCE_ID_LEN = 2
DEPARTMENT_ID_LEN = 5
MUNICIPALITY_ID_LEN = 6
LOCALITY_ID_LEN = 11
STREET_ID_LEN = 13
STREET_BLOCK_ID_LEN = 18

STREETS_SOURCE = 'INDEC'
