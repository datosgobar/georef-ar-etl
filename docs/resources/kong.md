# Kong

Kong es un API Management que facilita la creación, publicación, mantenimiento, monitoreo y protección de API a cualquier escala.

## Instalación

- Instalar dependencias

    `$ sudo apt-get update`
    
    `$ sudo apt-get install openssl libpcre3 procps perl http curl`

- Descargar e instalar paquete  

    `$ wget https://bintray.com/kong/kong-community-edition-deb/download_file?file_path=dists/kong-community-edition-0.11.0.xenial.all.deb`
    
    `$ sudo dpkg -i kong-community-edition-0.11.0.xenial.all.deb`

## Base de datos    

- Crear base de datos y usuario

    ```postgresplsql
      CREATE USER kong; 
      CREATE DATABASE kong OWNER kong;
    ```

- Migraciones

    `$ sudo kong migrations up [-c /path/to/kong.conf]`
    
## Kong

- Levantar Kong

    `$ sudo kong start [-c /path/to/kong.conf]`
    
    
- Test
    
    `$ export KONG_HOST=127.0.0.1`
    
    `$ http $KONG_HOST:8000`
    
- Registrar Georef API

    `$ export GEOREF_API_URL=http://181.209.63.243:5000/api/v1.0`
    
    `$ http $GEOREF_API_URL'/calles'`

    `$ http POST $KONG_HOST:8001/apis name=demo hosts=$KONG_HOST upstream_url=$GEOREF_API_URL`
    
    `$ http $KONG_HOST:8000/calles`
    
- Activar plugin JWT

    `$ http POST $KONG_HOST:8001/apis/demo/plugins name=jwt config.secret_is_base64=true`

    `$ http $KONG_HOST:8000/calles`
    
    
- Crear usuario

    `$ http POST $KONG_HOST:8001/consumers username=<user>`
  
  
- Crear credenciales JWT

    `$ curl -H "Content-Type: application/json" -X POST -d '{}' $KONG_HOST:8001/consumers/<name>/jwt`


- Listar usuarios o un usuario en particular

    `$ http $KONG_HOST:8001/consumers/`

    `$ http $KONG_HOST:8001/consumers/<user>/jwt`

    
- Generar Token: https://jwt.io/

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
        ) x secret base64 encoded
        ```
        

- Consumir APIS

    `$ http $KONG_HOST:8000/calles 'Authorization:Bearer <token>'`
  
- Rate limits

    `$ http POST $KONG_HOST:8001/apis/demo/plugins name=rate-limiting consumer_id=<consumer_id> config.hour=1`

## Docker

[Demo](https://programar.cloud/post/demo-del-api-gateway-kong/)
