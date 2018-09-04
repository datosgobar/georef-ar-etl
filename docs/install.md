# Deploy georef-ar-etl

## Dependencias

- [Gedal](http://www.gdal.org/index.html)
- [PostgreSQL 9.6](https://www.postgresql.org/download/)
- [PostGis 2.3](http://postgis.net/install/)
- [Python >=3.6.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Unzip
- [Virtualenv](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/)
- Wget

## Base de datos

Crear **dos bases de datos** en PostgreSQL, ambas con la extensión Postgis. No es requerido que se encuentren en el mismo clúster.

Ejemplo:

```plsql
  -- Creando una base de datos
  CREATE DATABASE georef_data WITH ENCODING='UTF8';

  -- Agregando Postgis a la base de datos creada
  CREATE EXTENSION postgis;
```

```plsql
  -- Creando una base de datos
  CREATE DATABASE georef WITH ENCODING='UTF8';

  -- Agregando Postgis a la base de datos creada
  CREATE EXTENSION postgis;
```

## Instalación

1. Clonar repositorio:

    `$ git clone https://github.com/datosgobar/georef-ar-etl.git`

2. Copiar el archivo de ejemplo con las variables de entorno:

    `$ cp environment.example.sh environment.sh`

3. Completar el _script_ `environment.sh` con los valores de configuración correspondientes:

    `$ vi environment.sh`

    - Configuraciones de aplicación

        ```bash
        export DEBUG= # True | False
        export DJANGO_SECRET_KEY='' # valor obligatorio
        ```

        Nota: [Ver instructivo para crear _secret key_](python3.6.md#secret-key)

    - Configuraciones de [bases de datos previamente creadas](#base-de-datos)

        ```bash
        export GEOREF_DB_HOST= # localhost
        export GEOREF_DB_NAME= # georef
        export GEOREF_DB_USER= # user
        export GEOREF_DB_PASS= # password
        export POSTGRES_HOST= # localhost
        export POSTGRES_DBNAME= # georef_data
        export POSTGRES_USER= # user
        export POSTGRES_PASSWORD= # password
        ```

4. Para instalar georef-ar-etl, actualizar y generar los datos de unidades territoriales y vías de circulación, ejecutar el siguiente comando:

    `$ make all`
    
    Otro comandos útiles:
    
    `$ make help`
