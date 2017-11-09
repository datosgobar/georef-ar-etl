# Deploy Georef

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

Crear **dos** base de datos en PostgreSQL ambas con la extensión Postgis (no es requerido que se encuentren en el mismo clúster)

Ejemplo:

```plsql
  -- Creando una base de datos
  CREATE DATABASE indec;
  
  -- Agregando Postgis a la base de datos creada
  CREATE EXTENSION postgis;
```

```plsql
  -- Creando una base de datos
  CREATE DATABASE georef;
  
  -- Agregando Postgis a la base de datos creada
  CREATE EXTENSION postgis;
```

## Instalación

1. Clonar repositorio

    `$ git clone https://github.com/datosgobar/georef.git`

2. Instalar dependencias con pip

    `$ pip install -r requirements.txt`

3. Copiar las variables de entorno

    `$ cp environment.example.sh environment.sh`

4. Completar los valores con los datos correspondientes:

    ```bash
      export GEOREF_API_URL= # URL

      export POSTGRES_HOST= # localhost
      export POSTGRES_DBNAME= # indec
      export POSTGRES_USER= # user
      export POSTGRES_PASSWORD= # password
      
      export GEOREF_HOST= # localhost
      export GEOREF_DB_NAME= # georef
      export GEOREF_DB_USER= # user
      export GEOREF_DB_PASS= # password
    ```
5. Exportar las variables de entorno

    `$ source environment.sh`

6. Ejercutar el script `etl_indec_vias.sh` para descargar e importar los datos de INDEC:

    ```bash
      cd etl_scripts/
      sh etl_indec_vias.sh
    ```

7. Sincronizar la base de datos

    `$ ./manage.py migrate`

8. Cargar datos de entidades y vías

    `$ ./manage.py runscript load_entities`

    `$ ./manage.py runscript load_roads`


## ElasticSearch

1. Levantar el servicio de ElasticSearch

    `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
    
2. Crear índices

    `$ ./manage.py runscript index_entities`

    `$ ./manage.py runscript index_roads`

3. Verificar índices

    `$ curl localhost:9200/_cat/indices?v`

- Para borrar índices puede usarse el comando

    `$ ./manage.py runscript delete_index --script-args <índice>`

## Correr App

- Levantar servidor

    `$ ./manage.py runserver`
