# Georef - Guía de instalación para entorno de desarrollo

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- [Gedal](http://www.gdal.org/index.html)
- [PostgreSQL 9.6](https://www.postgresql.org/download/)
- [PostGis 2.3](http://postgis.net/install/)
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Unzip
- Wget

## Base de datos

Crear **dos** bases de datos en PostgreSQL, ambas con la extensión Postgis (no es requerido que se encuentren en el mismo clúster).

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

4. Completar los valores con los datos correspondientes:

    ```bash
    export GEOREF_API_URL= # URL
    export GEOREF_HOST= # localhost
    export GEOREF_DB_NAME= # georef
    export GEOREF_DB_USER= # user
    export GEOREF_DB_PASS= # password
    
    export POSTGRES_HOST= # localhost
    export POSTGRES_DBNAME= # indec
    export POSTGRES_USER= # user
    export POSTGRES_PASSWORD= # password
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
