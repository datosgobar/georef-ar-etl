#!/usr/bin/env bash

set -o errexit
set -o nounset

if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ] 
then

 # Verifica la conexión

 # Geoserver INDEC
 URL='http://geoservicios.indec.gov.ar/geoserver/sig/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=sig:vias&outputFormat=SHAPE-ZIP'
 FILE='vias.zip'

 # Descarga la capa "Vías de circulación" en formato SHAPEFILE
 wget --progress=dot -e dotbytes=1M -O ${FILE} ${URL}

 # Descomprime
 unzip ${FILE} -d vias

 # Carga la capa Vías a PostgreSQL
 export SHAPE_ENCODING="LATIN1"

 ogr2ogr -append -f "PostgreSQL" \
     PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
     vias/vias.shp -nln indec_vias -nlt MULTILINESTRING -lco GEOMETRY_NAME=geom

 # Genera log de actividad
 echo "--------------------------------------------------------------------------- $(date)" >> indec.log
 ogrinfo -ro -so vias/vias.shp -al >> indec.log
 echo "Listo"
fi
