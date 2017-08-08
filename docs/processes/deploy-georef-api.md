# Deploy Georef API

## Variables de entorno

- Archivo `environment.sh`

```
  export FLASK_APP=service/__init__.py
  export GEOREF_URL='http://127.0.0.1:5000/api/v1.0/'
  export OSM_API_URL='http://nominatim.openstreetmap.org/search'
  export POSTGRES_DBNAME=georef
  export POSTGRES_PASSWORD=postgres
  export POSTGRES_USER=postgres
  export FLASK_DEBUG=1
```

## ElasticSearch

[Instalación](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)

- Levantar el servicio de ElasticSearch

  `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
  
  
- Listar índices

  `$ curl localhost:9200/_cat/indices?v`
  
  
- Crear índices

    `$ curl -XPOST 'localhost:9200/provincias/provincia/_bulk?pretty&refresh' -H 'Content-Type: application/json' --data-binary '@provincias.json'`

    `$ curl -XPOST 'localhost:9200/departamentos/departamento/_bulk?pretty&refresh' -H 'Content-Type: application/json' --data-binary '@departamentos.json'`

    `$ curl -XPOST 'localhost:9200/localidades/localidad/_bulk?pretty&refresh' -H 'Content-Type: application/json' --data-binary '@localidades.json'`

    `$ curl -XPOST 'localhost:9200/calles/calle/_bulk?pretty&refresh' -H 'Content-Type: application/json' --data-binary '@caba_streets.json'`

    `$ curl -XPOST 'localhost:9200/calles/calle/_bulk?pretty&refresh' -H 'Content-Type: application/json' --data-binary '@indec_streets.json'`


- Borrar índices

  `$ curl -XDELETE 'localhost:9200/caba?pretty&pretty'`

## API 

- Correr app

  `$ ./runserver.sh`
  
  
- Test

  `$ python -m unittest tests/normalization.py`
  
  `$ python -m unittest tests/parsing.py`
  

- Consumir mediante la herramienta CURL:

  `$ curl localhost:5000/api/v1.0/direcciones?direccion=cabral`
  
  `$ curl localhost:5000/api/v1.0/provincias`
