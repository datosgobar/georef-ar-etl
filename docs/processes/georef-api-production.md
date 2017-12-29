# Deploy Georef API

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- Gunicorn
- [Nginx](https://nginx.org/)
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Postgresql Client Common
- [Virtualenv](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/)

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
    export GEOREF_DB_HOST= # 'localhost'
    export GEOREF_DB_NAME= # georef 
    export GEOREF_DB_USER= # user
    export GEOREF_DB_PASS= # password
    ```
 
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

    `$ curl -X GET 'http://localhost:9200'`

## Correr API 

Agregar la configuración de los servicios `gunicorn` y `nginx`.

1. Configurar servicio en `/etc/systemd/system/`. Completar y modificar el archivo `georef-api.service` de este repositorio.

2. Levantar el servicio:

    `# systemctl start georef-api.service`

3. Para `nginx`, crear `/etc/nginx/sites-available/georef-api` tomando como base la configuración del archivo `georef-api.nginx`.

4. Generar un link simbólico a la configuración del sitio:

    `# ln -s /etc/nginx/sites-available/georef-api /etc/nginx/sites-enabled`,

5. Validar configuración:

    `# nginx -t`

6. Cargar la nueva configuración:

    `# nginx -s reload`

7. Correr Nginx:

    `# nginx`

## Pruebas

- Test

  `(venv) $ python -m unittest tests/test_normalization.py`
  
  `(venv) $ python -m unittest tests/test_parsing.py`
  
- Consumir mediante la herramienta CURL:

  `$ curl localhost/api/v1.0/direcciones?direccion=cabral+500`
  
  `$ curl localhost/api/v1.0/provincias`
