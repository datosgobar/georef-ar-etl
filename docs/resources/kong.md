# Kong

Kong es un API Management que facilita la creación, la publicación, el mantenimiento, la monitorización y la protección de API a cualquier escala.

## Start with Docker

[Fuente](https://programar.cloud/post/demo-del-api-gateway-kong/)

- Instalando Kong

 `$ sudo docker run -d --name kong-database -p 5433:5433 -e POSTGRES_USER=kong -e POSTGRES_DB=kong postgres:9.4`
 
 `$ sudo docker run -d --name kong --link kong-database:kong-database -e KONG_DATABASE=postgres -e KONG_PG_HOST=kong-database -p 8000:8000 -p 8443:8443 -p 8001:8001 -p 7946:7946 -p 7946:7946/udp kong:0.10.1`
 
 - Declarando variable

  `$ HOST_KONG=127.0.0.1`
  
  `$ http $HOST_KONG:8000`
  
- Registrando una API en kong

  `$ http POST $HOST_KONG:8001/apis name=demo hosts=$HOST_KONG upstream_url=http://httpbin.org`
  
  `$ http $HOST_KONG:8000/headers Host:$HOST_KONG`
  
- Activar plugin JWT

  `$ http POST $HOST_KONG:8001/apis/demo/plugins name=jwt`
  
  `$ http $HOST_KONG:8000/headers Host:$HOST_KONG`
  
- Crear usuario

  `$ http POST $HOST_KONG:8001/consumers username=mcardozo custom_id=cardozo.marisolp@gmail.com`
  
  
  ```json  
    {
        "algorithm": "HS256", 
        "consumer_id": "59644cad-7ef2-46a6-b282-440a5481d9d0", 
        "created_at": 1500559371000, 
        "id": "c3f5019b-fc9f-47ca-b86b-08c4b7926f8e", 
        "key": "bf7d2a77f34644c9a017f87ae5c654fe", 
        "secret": "92120a0d4a534480857d67cedaf7499d"
    }
   ```
  
- Mostrar las credenciales

  `$ http POST $HOST_KONG:8001/consumers/mcardozo/jwt Content-Type:application/x-www-form-urlencoded`
  
- Listar usuarios o un usuario en particular

  `$  http $HOST_KONG:8001/consumers/`

  `$ http $HOST_KONG:8001/consumers/mcardozo/jwt`
  
  
- Consumir APIS

  `$ http $HOST_KONG:8000/headers Host:$HOST_KONG 'Authorization:Bearer <token>'`
  
- Rate limits

  `$ http POST $HOST_KONG:8001/apis/demo/plugins name=rate-limiting consumer_id=59644cad-7ef2-46a6-b282-440a5481d9d0 config.hour=1`
