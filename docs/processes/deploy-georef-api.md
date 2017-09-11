# Deploy Georef API

## Sesión tmux

`$ tmux new -s georef-api`  para crear la sesión.

`$ tmux attach -t georef-api` para ingresar a la sesión.

`$ Ctrl + B, D` para salir de la sesión sin terminarla.

## Variables de entorno

- Copiar archivo `environment.example.sh` a `environment.sh` y completar los valores:

```
  export FLASK_APP=service/__init__.py
  export GEOREF_URL=<URL>
  export OSM_API_URL='http://nominatim.openstreetmap.org/search'
  export POSTGRES_DBNAME=<database>
  export POSTGRES_PASSWORD=<user>
  export POSTGRES_USER=<password>
```

## ElasticSearch

[Instalación](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)

- Levantar el servicio de ElasticSearch

  `$ cd path/to/elasticsearch/bin/ && ./elasticseach`
  
  
- Listar índices

  `$ curl localhost:9200/_cat/indices?v`

- Borrar índices

  `$ curl -XDELETE 'localhost:9200/<nombre_indice>?pretty&pretty'`

## API 

- Correr app

  `$ ./runserver.sh`
  
  
- Test

  `$ python -m unittest tests/normalization.py`
  
  `$ python -m unittest tests/parsing.py`
  

- Consumir mediante la herramienta CURL:

  `$ curl localhost:5000/api/v1.0/direcciones?direccion=cabral`
  
  `$ curl localhost:5000/api/v1.0/provincias`
