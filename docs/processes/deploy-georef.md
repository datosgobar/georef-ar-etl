# Deploy Georef

## Python y Virtualenv

- Ver guía de instalación de Python3.6.

    `$ pip install -r requirements.txt`

## Variables de entorno

- Archivo `environment.sh`

```
export GEOREF_API_URL=http://localhost:5000/api/v1.0/
export POSTGRES_HOST=localhost
export POSTGRES_DBNAME=<database>
export POSTGRES_USER=<user>
export POSTGRES_PASSWORD=<password>
```

-  Bajar datos de INDEC y crear base de datos intermedia.

    `sh etl_scripts/etl_indec_vias.sh`

## Django app

- Crear base de datos

    `$ cd ./manage.py makemigrations`

    `$ cd ./manage.py migrate`

-  Cargar datos de entidades y vías

    `$ cd ./manage.py runscript load_states`

    `$ cd ./manage.py runscript load_departments`

    `$ cd ./manage.py runscript load_localities`

    `$ cd ./manage.py runscript load_roads`

- Correr tests

    `$ ./manage.py test`

- Correr app

    `$ ./manage.py runserver`

## ElasticSearch

- Levantar el servicio de ElasticSearch

    `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
    
- Crear índices

    `$ ./manage.py runscript index_entities`

    `$ ./manage.py runscript index_roads`

- Listar índices

    `$ curl localhost:9200/_cat/indices?v`
