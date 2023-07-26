"""Módulo 'constants' de georef-ar-etl.

Define valores constantes utilizados en distintas partes del ETL.

"""

from enum import Enum

ETL_VERSION = '12.1.0'
DATA_DIR = 'data'
CONFIG_PATH = 'config/georef.cfg'
DIR_PERMS = 0o700


class BAHRAType(Enum):
    E = 'Entidad'
    LC = 'Componente de localidad compuesta'
    LS = 'Localidad simple'
    ST = 'Sitio edificado'
    LSE = 'Localidad simple con entidad'
    LCE = 'Componente de localidad compuesta con entidad'


# Todos los tipos BAHRA
BAHRA_TYPES = {
    member.name: member.value
    for member in list(BAHRAType)
}


# Subconjunto de tipos BAHRA seleccionados para crear el dataset de localidades
LOCALITY_TYPES = {
    member.name
    for member in [
        BAHRAType.E,
        BAHRAType.LC,
        BAHRAType.LS,
        BAHRAType.LCE,
        BAHRAType.LSE
    ]
}

# Las localidades censales son LC o LC, en todos los casos (campo tiploc)
CENSUS_LOCALITY_TYPES = {
    '1': BAHRAType.LS.name,
    '2': BAHRAType.LC.name
}

# Valores posible para campo func_loc
CENSUS_LOCALITY_ADMIN_FUNCTIONS = {
    '1': 'CAPITAL_PAIS',
    '2': 'CAPITAL_PROVINCIA',
    '3': 'CABECERA_DEPARTAMENTO',
    '0': None
}

# El único tipo de calle que *no* queremos utilizar.
STREET_TYPE_OTHER = 'OTRO'

# Se necesitan saber los IDs de las provincias antes de ejecutar el ETL, en
# el archivo streets.py.
CABA_PROV_ID = '02'
PROVINCE_IDS = [
    CABA_PROV_ID, '06', '10', '14', '18', '22', '26', '30', '34', '38', '42',
    '46', '50', '54', '58', '62', '66', '70', '74', '78', '82', '86', '90',
    '94'
]

# Existen dos numeraciones posibles para las comunas de CABA:
# - Del 1 al 15,
# - Del 7 al 105 (1 * 7 al 15 * 7)
# La numeración preferida es la del 7 al 105, por lo que los datos que usan la
# numeración alterna deben ser modificados para usar la otra. Para lograr esto,
# se multiplica por 7 el ID de la comuna (ver resolución 55/2019 de INDEC).
CABA_MULT_FACTOR = 7

# Algunas entidades geográficamente dentro de CABA tienen IDs que comienzan con
# '02000', haciendo referencia a un departamento '000' de la provincia '02'
# (CABA). Como este departamento no existe, se acepta este ID de departamento
# como válido cuando se crean entidades, pero en la relación (modelo y DB) se
# almacena NULL como departamento.
CABA_VIRTUAL_DEPARTMENT_ID = '02000'

# CABA solo contiene una localidad censal, sin embargo las calles de CABA
# contienen IDs que *no* comienzan con el ID de esta localidad censal.
# Entonces, necesario contar con el ID de la localidad censal para buscarla en
# la base.
CABA_CENSUS_LOCALITY = '02000010'

PROVINCES = 'provincias'
DEPARTMENTS = 'departamentos'
MUNICIPALITIES = 'municipios'
SETTLEMENTS = 'asentamientos'
LOCALITIES = 'localidades'
CENSUS_LOCALITIES = 'localidades_censales'
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
SETTLEMENTS_ETL_TABLE = ETL_TABLE_NAME.format(SETTLEMENTS)
LOCALITIES_ETL_TABLE = ETL_TABLE_NAME.format(LOCALITIES)
CENSUS_LOCALITIES_ETL_TABLE = ETL_TABLE_NAME.format(CENSUS_LOCALITIES)
STREETS_ETL_TABLE = ETL_TABLE_NAME.format(STREETS)
INTERSECTIONS_ETL_TABLE = ETL_TABLE_NAME.format(INTERSECTIONS)
STREET_BLOCKS_ETL_TABLE = ETL_TABLE_NAME.format(STREET_BLOCKS)

PROVINCES_TMP_TABLE = TMP_TABLE_NAME.format(PROVINCES)
DEPARTMENTS_TMP_TABLE = TMP_TABLE_NAME.format(DEPARTMENTS)
MUNICIPALITIES_TMP_TABLE = TMP_TABLE_NAME.format(MUNICIPALITIES)
SETTLEMENTS_TMP_TABLE = TMP_TABLE_NAME.format(SETTLEMENTS)
CENSUS_LOCALITIES_TMP_TABLE = TMP_TABLE_NAME.format(CENSUS_LOCALITIES)
STREETS_TMP_TABLE = TMP_TABLE_NAME.format(STREETS)
STREET_BLOCKS_TMP_TABLE = TMP_TABLE_NAME.format(STREET_BLOCKS)

PROVINCE_ID_LEN = 2
DEPARTMENT_ID_LEN = 5
MUNICIPALITY_ID_LEN = 6
SETTLEMENT_ID_LEN = 11
LOCALITY_ID_LEN = SETTLEMENT_ID_LEN
CENSUS_LOCALITY_ID_LEN = 8
STREET_ID_LEN = 13
STREET_BLOCK_ID_LEN = 18

STREETS_SOURCE = 'INDEC'
CENSUS_LOCALITIES_SOURCE = 'INDEC'

RECIPIENTS_PREFIX = 'recipients_'
PROCESS_SUB_PREFIX = '_process-'
START_PROCESS_PREFIX = 'start' + PROCESS_SUB_PREFIX
END_PROCESS_PREFIX = 'end' + PROCESS_SUB_PREFIX