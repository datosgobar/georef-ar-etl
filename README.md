# georef-ar-etl
[![Build Status](https://travis-ci.org/datosgobar/georef-ar-etl.svg?branch=master)](https://travis-ci.org/datosgobar/georef-ar-etl)
[![Coverage Status](https://coveralls.io/repos/github/datosgobar/georef-ar-etl/badge.svg?branch=master)](https://coveralls.io/github/datosgobar/georef-ar-etl?branch=master)
![](https://img.shields.io/github/license/datosgobar/georef-ar-etl.svg)
![](https://img.shields.io/badge/python-3-blue.svg)

Segunda iteración del ETL del Servicio de Normalización de Datos Geográficos de Argentina. La primera iteración puede encontrarse bajo la rama [`etl-legacy`](https://github.com/datosgobar/georef-ar-etl/tree/etl-legacy).

## Documentación
Para consultar la documentación del ETL, acceder a [https://datosgobar.github.io/georef-ar-api/etl/](https://datosgobar.github.io/georef-ar-api/etl/).

## Contenedores
Para correr los contenedores asegúrate de tener instalado docker-compose\
El archivo de configuración puede correr tres servicios creando los siguientes contenedores:
- georef-etl_db: Un contenedor con postgres y postgis para almacenar los datos procesados. Estos datos son almacenados y persistidos en un volumen de docker. La configuración de credenciales de la base de datos se lee del archivo .env
- georef-etl_app: Un contenedor con la aplicación. Al correrlo la primera vez, o después de modificar algún modelo, es necesario correr una migración.
- georref-etl_db_test: En forma optativa se puede levantar el tercer contenedor para correr las pruebas del ETL

Antes de levantar los servicios de la base de datos deberás configurar las variables de entorno en el archivo .env\
Antes de levantar el servicio de la app deberás generar un archivo georef.cfg que puedes hacerlo copiando el que ya existe (georef.example.cfg) y renombrando ciertas variables. Tener en cuenta que los dominios para los servidores de postgres de la base de datos del ETL y de la base de datos de pruebas son "db" y "db-test" respectivamente; los puertos son 5432 por defecto y las credenciales deberán ser las mismas que las definidas en el archivo docker/.env\
Las carpetas config/; files/ y reports/ serán montadas dentro del contenedor de georef-etl_app y se podrá acceder desde el host a los archivos generados por la app.

### Correr la aplicación:

Situarse dentro de la carpeta "docker"

Para correr la aplicación la primera vez:

```
docker-compose up -d app
docker-compose run app make migrate
docker-compose run app make run
```

Si se modifica el código fuente reconstruir la imagen

`docker-compose build app`

Para correr los test

```
docker-compose up db-test
docker-compose run app python -m unittest -v
```

## Soporte
En caso de que encuentres algún bug, tengas problemas con la instalación, o tengas comentarios de alguna parte de Georef ETL, podés mandarnos un mail a [datosargentina@jefatura.gob.ar](mailto:datosargentina@jefatura.gob.ar) o [crear un issue](https://github.com/datosgobar/georef-ar-etl/issues/new?title=Encontre-un-bug-en-georef-ar-etl).
