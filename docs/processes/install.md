# Deploy Georef

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

Crear **dos** bases de datos en PostgreSQL, ambas con la extensión Postgis. No es requerido que se encuentren en el mismo clúster ni alojadas en el mismo _host_ de la aplicación.

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

1. Clonar repositorio

    `$ git clone https://github.com/datosgobar/georef-etl.git`

2. Crear entorno virtual e instalar dependencias con pip

    `$ python3.6 -m venv venv`

    `(venv)$ pip install -r requirements.txt`

3. Copiar las variables de entorno

    `(venv)$ cp environment.example.sh environment.sh`

4. Completar el script `environment.sh` con los valores de configuración correspondientes:

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

5. Exportar las variables de entorno

    `(venv)$ . environment.sh`

6. Ejecutar el script `etl_indec_vias.sh` y `etl_ign_entities.sh` para descargar e importar los datos de INDEC e IGN:

    ```bash
    cd etl_scripts/
    ./etl_ign_entidades.sh
    ./etl_indec_vias.sh
    ```

7. Sincronizar la base de datos

    `(venv)$ ./manage.py migrate`

8. Cargar datos de entidades y vías

    `(venv)$ ./manage.py runscript load_entities`

    `(venv)$ ./manage.py runscript load_roads`
    
9. Generar datos de entidades y vías de circulación en formato Json

    `(venv)$ ./manage.py runscript create_entities_data`
    
    `(venv)$ ./manage.py runscript create_roads_data`
