# Deploy Georef API

## Dependencias

- [ElasticSearch >=5.5](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
- Gunicorn
- [Python >=3.5.x](https://www.python.org/downloads/)
- [Pip](https://pip.pypa.io/en/stable/installing/)
- Postgresql Client Common

## Instalación

1. Clonar repositorio

    `$ git clone https://github.com/datosgobar/georef-api.git`
    
2. Crear entorno virtual e instalar dependencias con pip

    `$ python3.6 -m venv venv`
    
    `(venv)$ pip install -r requirements.txt`
 
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

  `$ python -m unittest tests/test_normalization.py`
  
  `$ python -m unittest tests/test_parsing.py`
  
- Consumir mediante la herramienta CURL:

  `$ curl localhost:5000/api/v1.0/direcciones?direccion=cabral`
  
  `$ curl localhost:5000/api/v1.0/provincias`
