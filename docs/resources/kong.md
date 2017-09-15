# Kong

Kong es un API Management que facilita la creación, la publicación, el mantenimiento, la monitorización y la protección de API a cualquier escala.

## Instalación

- Instalar dependencias

    `$ sudo apt-get update`
    
    `$ sudo apt-get install openssl libpcre3 procps perl http`

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
    
    `$ export HOST_KONG=127.0.0.1`
    
    `$ http $HOST_KONG:8000`
    
- Registrar Georef API

    `$ export GEOREF_API_URL=http://181.209.63.243:5000/api/v1.0`
    
    `$ http $GEOREF_API_URL'/calles'`

    `$ http POST $HOST_KONG:8001/apis name=demo hosts=$HOST_KONG upstream_url=$GEOREF_API_URL`
    
    `$ http $HOST_KONG:8000/calles`
    
- Activar plugin JWT

    `$ http POST $HOST_KONG:8001/apis/demo/plugins name=jwt`

    `$ http $HOST_KONG:8000/calles`
    
    
- Crear usuario

    `$ http POST $HOST_KONG:8001/consumers username=<user> custom_id=<example:email>`
  
- Mostrar credenciales

    `$ http POST $HOST_KONG:8001/consumers/<user>/jwt Content-Type:application/x-www-form-urlencoded`


- Listar usuarios o un usuario en particular

    `$ http $HOST_KONG:8001/consumers/`

    `$ http $HOST_KONG:8001/consumers/<user>/jwt`

- Consumir APIS

    `$ http $HOST_KONG:8000/calles Host:$HOST_KONG 'Authorization:Bearer <token>'`
  
- Rate limits

    `$ http POST $HOST_KONG:8001/apis/demo/plugins name=rate-limiting consumer_id=59644cad-7ef2-46a6-b282-440a5481d9d0 config.hour=1`


## Docker

[Demo](https://programar.cloud/post/demo-del-api-gateway-kong/)