# Kong

Kong es un API Management que facilita la creación, publicación, mantenimiento, monitoreo y protección de API a cualquier escala.

## Instalación

[Documentación oficial](https://getkong.org/install/)


## Instalación en Ubuntu xenial

- Instalar dependencias

    `$ sudo apt-get update`
    
    `$ sudo apt-get install openssl libpcre3 procps perl http`

- Descargar e instalar paquete 

    `$ wget -O kong-community-edition-0.11.0.xenial.all.deb \
    https://bintray.com/kong/kong-community-edition-deb/download_file?file_path=dists/kong-community-edition-0.11.0.xenial.all.deb`
    
    `$ sudo dpkg -i kong-community-edition-0.11.0.xenial.all.deb`
    
- Generar archivo de configuración y modificar en caso de ser necesario

    `$ cp /etc/kong/kong.conf.default /etc/kong/kong.conf`

## Base de datos    

- Crear base de datos y usuario (PostgreSQL)

    ```postgresplsql
      CREATE USER kong; 
      CREATE DATABASE kong OWNER kong;
    ```

- Migraciones

    `$ sudo kong migrations up`
    
## Kong

- Levantar Kong

    `$ sudo kong start` 
    
- Test
    
    `$ export KONG_HOST=127.0.0.1`
    
    `$ http $KONG_HOST:8000`
    
- Registrar endpoint de provincias de Georef API

    `$ export GEOREF_API_PROVINCIAS=http://181.209.63.243:5000/api/v1.0/provincias`
        
    `$ http GEOREF_API_PROVINCIAS`

    `$ http POST $KONG_HOST:8001/apis name=provincias uris=/provincias upstream_url=$GEOREF_API_PROVINCIAS`
       
    `$ http $KONG_HOST:8000/provincias`
        
- Activar plugin [JWT](https://getkong.org/plugins/jwt/)

    `$ http POST $KONG_HOST:8001/apis/provincias/plugins name=jwt config.secret_is_base64=true`

    `$ http $KONG_HOST:8000/provincias`
    
- Crear usuario

    `$ http POST $KONG_HOST:8001/consumers username=<user>`
  
  
- Crear credenciales JWT

    `$ echo '{}' | http POST $KONG_HOST:8001/consumers/<user>/jwt`


- Listar usuarios o un usuario en particular

    `$ http $KONG_HOST:8001/consumers/`

    `$ http $KONG_HOST:8001/consumers/<user>/jwt`

    
- Generar Token:
 
    1. [Debugger](https://jwt.io/)

        - HEADER
        
            ```json
            {
              "alg": "HS256",
              "typ": "JWT"
            }
            ```
            
        - PAYLOAD: `"iss": "<key>"`
        
        - VERIFY SIGNATURE
        
            ```
            HMACSHA256(
             base64UrlEncode(header) + "." +
             base64UrlEncode(payload),
             <secret>
            ) [x] secret base64 encoded // check
            ```
    2. [PyJwt](https://github.com/jpadilla/pyjwt)
    
        ```python
        import jwt
        import base64
        
        encoded = jwt.encode({'iss': '<key>'}, base64.b64decode(b'<secret>'), algorithm='HS256')
        print(encoded)
        ```

- Consumir APIS

    `$ http $KONG_HOST:8000/provincias 'Authorization:Bearer <token>'`
  
- [Rate limits](https://getkong.org/plugins/rate-limiting/)

    `$ http POST $KONG_HOST:8001/apis/provincias/plugins name=rate-limiting consumer_id=<consumer_id> config.minute=10`

- [StatsD](https://getkong.org/plugins/statsd/)

    `$ http POST $KONG_HOST:8001/apis/provincias/plugins name=statsd consumer_id=<consumer_id>`
