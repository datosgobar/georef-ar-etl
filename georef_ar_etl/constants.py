RAW_TABLE_NAME = 'raw_{}'
ETL_TABLE_NAME = 'georef_{}'

PROVINCES_ETL_TABLE = ETL_TABLE_NAME.format('provincias')
DEPARTMENTS_ETL_TABLE = ETL_TABLE_NAME.format('departamentos')

PROVINCES_RAW_TABLE = RAW_TABLE_NAME.format('provincias')
DEPARTMENTS_RAW_TABLE = RAW_TABLE_NAME.format('departamentos')

PROVINCE_ID_LEN = 2
DEPARTMENT_ID_LEN = 5