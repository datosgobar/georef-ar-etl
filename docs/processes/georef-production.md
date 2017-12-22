# Deploy Georef

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- Gunicorn
- [Gedal](http://www.gdal.org/index.html)
- [Nginx](https://nginx.org/)
- [PostgreSQL 9.6](https://www.postgresql.org/download/)
- [PostGis 2.3](http://postgis.net/install/)
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Unzip
- [Virtualenv](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/)
- Wget

## Base de datos

Crear **dos** bases de datos en PostgreSQL, ambas con la extensión Postgis. No es requerido que se encuentren en el mismo clúster ni alojadas en el mismo _host_ de la aplicación.

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

    `(venv)$ pip install -r requirements.txt`

3. Copiar las variables de entorno

    `$ cp environment.example.sh environment.sh`

4. Completar el script `environment.sh` con los valores de configuración correspondientes:

    - Configuraciones de aplicación

        ```bash
        export DJANGO_ENVIRONMENT= # opciones: dev | prod
        export DJANGO_SECRET_KEY= # valor obligatorio
        ```

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

    - Configuraciones para la integración de los servicios KONG y HERE(opcional)

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

    `$ ./manage.py migrate`

8. Cargar datos de entidades y vías

    `$ ./manage.py runscript load_entities`

    `$ ./manage.py runscript load_roads`

9. Cargar función para geocodificar direcciones

    `(venv) $ ./manage.py runscript load_geocode_function`

## ElasticSearch

1. Instalar dependencias JDK version 1.8.0_131

    `$ sudo apt install default-jre`

2. Instalar eleasticSearch

    `$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.0.deb`

    `# dpkg -i elasticsearch-6.0.0.deb`

3. Configuraciones

    `$ sudo vi /etc/elasticsearch/elasticsearch.yml`

    ```
    cluster.name: georef
    node.name: node-1
    network.host: 0.0.0.0
    http.max_content_length: 100mb
    ```

4. Probar el servicio

    `$ sudo service elasticsearch start` 

    `$ curl -X GET 'http://localhost:9200'`

5. Crear índices de entidades y vías

    `(venv)$ ./manage.py runscript index_entities`

    `(venv)$ ./manage.py runscript index_roads`

## Correr App

Agregar la configuración de los servicios `gunicorn` y `nginx`.

1. Configurar servicio y socket en `/etc/systemd/system/`. Completar y modificar los archivos `georef.service` y `georef.socket` de este repositorio.

2. Habilitar y levantar el socket:

    `# systemctl enable georef.socket`

    `# systemctl start georef.socket`

3. Levantar el servicio:

    `# systemctl start georef.service`

4. Para `nginx`, crear `/etc/nginx/sites-available/georef` tomando como base la configuración del archivo `georef.nginx`.

5. Generar un link simbólico a la configuración del sitio:

    `# ln -s /etc/nginx/sites-available/georef /etc/nginx/sites-enabled`,

6. Validar configuración:

    `# nginx -t`

7. Cargar la nueva configuración:

    `# nginx -s reload`

8. Correr Nginx:

    `# nginx`
