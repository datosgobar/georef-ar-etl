# Deploy Georef

## Python y Virtualenv

- Ver guía de instalación de Python3.6.

    `$ pip install -r requirements.txt`

## Sesión tmux

`$ tmux new -s georef`  para crear la sesión.

`$ tmux attach -t georef` para ingresar a la sesión.

`$ Ctrl + B, D` para salir de la sesión sin terminarla.

## Variables de entorno

- Copiar archivo `environment.example.sh` a `environment.sh` y completar los valores:

```
export GEOREF_API_URL=<URL>
export POSTGRES_HOST=<host>
export POSTGRES_DBNAME=<database>
export POSTGRES_USER=<user>
export POSTGRES_PASSWORD=<password>
```

-  Bajar datos de INDEC y crear base de datos intermedia.

    `sh etl_scripts/etl_indec_vias.sh`

## Django app

- Crear base de datos

    `$./manage.py makemigrations`

    `$./manage.py migrate`

-  Cargar datos de entidades y vías

    `$./manage.py runscript load_states`

    `$./manage.py runscript load_departments`

    `$./manage.py runscript load_localities`

    `$./manage.py runscript load_roads`

- Correr tests

    `$./manage.py test`

- Correr app

    `$./manage.py runserver`

## ElasticSearch

- Levantar el servicio de ElasticSearch

    `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
    
- Crear índices

    `$ ./manage.py runscript index_entities`

    `$ ./manage.py runscript index_roads`

- Listar índices

    `$ curl localhost:9200/_cat/indices?v`
