# georef
Prototipo para probar APIs de servicios de geolocalización.

Este repositorio contiene código de una aplicación Django que presenta una interfaz para buscar direcciones en distintos servicios de geolocalización, y ver los resultados de cada uno en mapas embebidos.
El objetivo a mediano plazo es extender y usar esta aplicación como herramienta de pruebas y punto de referencia para comparar resultados de varios servicios de geolocalización.

## Índice 
* [Instalación](#instalación)
* [Uso de georef](#uso-de-georef)
* [Contacto](#contacto)

## Instalación

* Clonar el repositorio.
* Crear un entorno virtual con `Python3.6`.
* Instalar dependencias: `pip install -r requirements.txt`.
* Crear DB (SQLite o [PostgreSQL](#instalación-postgresql)): `python manage.py migrate`.
* Correr tests: `python manage.py test`.
* Correr app localmente: `python manage.py runserver`.
* Para el servidor de desarrollo:
    * Declarar variables de entorno y *Python PATH* en `environment.sh`.
    * Correr el script de ejecución: `sudo ./runserver.sh`.

### Instalación PostgreSQL

* Agregar repositorio:

    `$ sudo add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ {código_distribución}-pgdg main"`

    Ejemplo distribución Xenial:

    `$ sudo add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main"`
    
* Agregar clave pública del repositorio:

    `$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -`
    
* Actualizar repositorios e instalar PostgreSQL 9.6 y Postgis 2.3:

    `$ sudo apt-get update`
    `$ sudo apt-get install postgresql-9.6 postgresql-9.6-postgis-2.3`
    
* Verificar la instalacion y levantar el servicio

    `$ psql --version`
    `$ sudo service postgresql start`
    
* Crear la contraseña por defecto del usuario postgres:

    `# su - postgres`
    `$ psql`
    `postgres=# ALTER ROLE postgres WITH PASSWORD 'postgres';`
    `postgres=# \q`
    
* Habilitar el acceso por contraseña:
    1. Abrir el archivo pg_hba.conf
    
        `# nano /etc/postgresql/9.6/main/pg_hba.conf`
         
    2. Cambiar el método de acceso:
    
        `local   all             postgres                           peer`

        `local   all             all                                peer`

        por
    
        `local   all             postgres                           md5`
        
        `local   all             all                                md5`
        
    3. Reiniciar el servicio:
    
        `$ sudo service postgresql restart`
        
* Crear la base de datos georef:

    `$ psql -U postgres -c "CREATE DATABASE georef;"`
    
* Instalar la extensión postgis:

    `$ psql -U postgres -d georef -c "create extension postgis;"`
        
* Crear el usuario Django:

    `$ psql -U postgres -c "CREATE USER django WITH PASSWORD '<password>';"`
    
* Dar permisos al usuario "django" sobre la db "georef":

    `$ psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE "georef" to django;"`

* Finalmente, probar la conexión:

    `$ psql -U django -d georef`

## Uso de georef
Al levantar la aplicación y acceder a la URL (localhost:8000, por ejemplo), se verá un campo para ingresar direcciones y un botón para ejecutar la búsqueda, también posible usando la tecla *Enter*.
Se muestran resultados si los hubiera y, por cada uno de éstos, puede expandirse el cuadro para ver detalles y el mapa correspondiente.

## Contacto
Te invitamos a [crearnos un
issue](https://github.com/datosgobar/georef/issues/new?title=Encontre-un-bug-en-georef) en caso de que encuentres algún bug o tengas comentarios de alguna parte de `georef`. Para todo lo demás, podés mandarnos tu sugerencia o consulta a [datos@modernizacion.gob.ar](mailto:datos@modernizacion.gob.ar).
