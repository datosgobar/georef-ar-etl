# Georef API - Guía de instalación para entorno de desarrollo

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Postgresql Client Common

## Instalación

1. Clonar repositorio

    `$ git clone https://github.com/datosgobar/georef-api.git`
    
2. Crear entorno virtual e instalar dependencias con pip

    `$ python3.6 -m venv venv`
    
    `(venv)$ pip install -r requirements.txt`

3. Copiar las variables de entorno

    `$ cp environment.example.sh environment.sh`
    
4. Completar los valores con los datos correspondientes:

  ```bash
    export FLASK_APP=service/__init__.py
    export GEOREF_URL= # URL
    export OSM_API_URL='http://nominatim.openstreetmap.org/search'
    export GEOREF_HOST= # 'localhost'
    export GEOREF_DBNAME= # georef 
    export GEOREF_USER= # user
    export GEOREF_PASSWORD= # password
  ```
 
5. Levantar el servicio de ElasticSearch

  `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
  
6. Listar índices

  `$ curl localhost:9200/_cat/indices?v`

## ElasticSearch

- Levantar el servicio de ElasticSearch

  `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
  
- Listar índices

  `$ curl localhost:9200/_cat/indices?v`

- Borrar índices

  `$ curl -XDELETE 'localhost:9200/<nombre_indice>?pretty&pretty'`

## Correr API 

    `(venv)$ . environment.sh`

    `(venv)$ flask run`

## Pruebas

- Test

  `$ python -m unittest tests/test_normalization.py`
  
  `$ python -m unittest tests/test_parsing.py`
  
- Consumir mediante la herramienta CURL:

  `$ curl localhost:5000/api/v1.0/direcciones?direccion=cabral`
  
  `$ curl localhost:5000/api/v1.0/provincias`