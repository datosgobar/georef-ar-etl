# ElasticSearch & Docker

## ElasticSearch estandar

- Descargando la imagen de dockerhub oficial

  `$ sudo docker pull elasticsearch`

## ElasticSearch con 2 nodos

- Memoria virtual: cambiar el valor de maxmap

   `$ sudo sysctl -w vm.max_map_count=262144`


- Correr los nodos

    `$ sudo docker run -d --name es0 -p 9200:9200 elasticsearch`

    `$ sudo docker run -d --name es1 --link es0 -e UNICAST_HOSTS=es0 elasticsearch`
    
    `$ sudo docker run -d --name es2 --link es0 -e UNICAST_HOSTS=es0 elasticsearch`
