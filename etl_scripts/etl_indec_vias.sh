#!/usr/bin/env bash

set -o errexit
set -o nounset

echo '-- Verificando paquetes requeridos --\n'
if  ogr2ogr --version; then echo "---"; fi
if  psql --version; then echo "---"; fi
if  wget --version | grep 'GNU Wget' ; then echo "---"; fi
if  unzip -v | grep Info-ZIP ; then echo "---"; else exit 1; fi

# Valida que las variables estén decladaras
if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
then
 # Valida la conexión a la base de datos
 if ogrinfo "PG:host=${POSTGRES_HOST} dbname=${POSTGRES_DBNAME} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" | grep "successful"
 then
  # Geoserver INDEC
  URL='https://geoservicios.indec.gov.ar/geoserver/sig/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=sig:vias&outputFormat=SHAPE-ZIP'
  FILE='vias.zip'

  # Descarga la capa "Vías de circulación" en formato SHAPEFILE
  wget --progress=dot -e dotbytes=1M -O ${FILE} ${URL} --no-check-certificate

  # Descomprime
  unzip ${FILE} -d vias

  # Carga la capa Vías a PostgreSQL
  export SHAPE_ENCODING="LATIN1"

  ogr2ogr -overwrite -progress -f "PostgreSQL" \
     PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
     vias/vias.shp -nln indec_vias -nlt MULTILINESTRING -lco GEOMETRY_NAME=geom

  # Genera log de actividad
  echo "--------------------------------------------------------------------------- $(date)" >> indec.log
  ogrinfo -ro -so vias/vias.shp -al >> indec.log
  rm ${FILE}; rm -rf vias;
  echo "Terminado!"
  fi
fi
