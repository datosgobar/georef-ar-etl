# Georef - Guía de instalación para entorno de desarrollo

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- [Gedal](http://www.gdal.org/index.html)
- [PostgreSQL 9.6](https://www.postgresql.org/download/)
- [PostGis 2.3](http://postgis.net/install/)
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Unzip
- [Virtualenv](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/)
- Wget

## Base de datos

Crear **dos** bases de datos en PostgreSQL, ambas con la extensión Postgis.

Ejemplo:

```plsql
  -- Creando una base de datos
  CREATE DATABASE indec WITH ENCODING='UTF8';

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

    `$ git clone https://github.com/datosgobar/georef.git`

2. Crear entorno virtual e instalar dependencias con pip

    `$ python3.6 -m venv venv`

    `(venv) $ pip install -r requirements.txt`

3. Copiar las variables de entorno

    `$ cp environment.example.sh environment.sh`

4. Completar el script `environment.sh` con los valores de configuración correspondientes:

    - Configuraciones de aplicación

        ```bash
        export DJANGO_ENVIRONMENT= # opciones: dev | prod
        export DJANGO_SECRET_KEY= # valor obligatorio
        ```

        Nota: [Ver instructivo para crear _secret key_](python3.6.md#secret-key)

    - Configuraciones de [bases de datos previamente creadas](#base-de-datos)

        ```bash
        export GEOREF_API_URL= # Ejemplo: http://127.0.0.1:5000/api/v1.0/
        export GEOREF_DB_HOST= # localhost
        export GEOREF_DB_NAME= # georef
        export GEOREF_DB_USER= # user
        export GEOREF_DB_PASS= # password
        export POSTGRES_HOST= # localhost
        export POSTGRES_DBNAME= # indec
        export POSTGRES_USER= # user
        export POSTGRES_PASSWORD= # password
        ```

    - Configuraciones para la integración de los servicios [KONG](../resources/kong.md) y [HERE](https://developer.here.com/)(opcional)

        ```bash
        export KONG_URL= # Ejemplo: http://127.0.0.1:8000/
        export KONG_DB_USER= # user
        export KONG_DB_PASS= # password
        export KONG_HOST= # localhost
        export HERE_APP_CODE= # app_code
        export HERE_APP_ID= # app_id
        ```

5. Exportar las variables de entorno

    `$ . environment.sh`

6. Ejecutar el script `etl_indec_vias.sh` para descargar e importar los datos de INDEC:

    ```bash
    cd etl_scripts/
    sh etl_indec_vias.sh
    ```

7. Sincronizar la base de datos

    `(venv) $ ./manage.py migrate`

8. Cargar datos de entidades y vías

    `(venv) $ ./manage.py runscript load_entities`

    `(venv) $ ./manage.py runscript load_roads`

9. Cargar función para geocodificar direcciones

    `(venv) $ ./manage.py runscript load_geocode_function`

## ElasticSearch

1. Levantar el servicio de ElasticSearch

    `$ cd path/to/elasticsearch/bin/ && ./elasticseach`

2. Configuraciones

    `$ sudo vi path/to/elasticsearch/config/elasticsearch.yml`

    ```
    cluster.name: georef
    node.name: node-1
    ```

    `$ sudo vi /etc/sysctl.conf`

    ```
    vm.max_map_count=262144
    ```

3. Probar el servicio

    `$ curl -X GET 'http://localhost:9200'`

    `$ curl localhost:9200/_cat/indices?v`

4. Crear índices de entidades y vías

    `(venv)$ ./manage.py runscript index_entities`

    `(venv)$ ./manage.py runscript index_roads`

    `$ curl localhost:9200/_cat/indices?v`

## Correr App

- Correr el comando

    `(venv)$ ./manage.py runserver --insecure`
