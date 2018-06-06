#!/usr/bin/env bash

set -o errexit
set -o nounset

function output_message (){
    echo "$(date '+%H:%M:%S') | $1 | ${USER} | etl_indec_vias | $2" >> logs/etl_$(date '+%Y%m%d').log
}

function check_library(){
    if [[ -n $1 ]]
    then
        output_message "INFO" "$1"
    else
        output_message "ERROR" "Error de dependecias"
        exit 1
    fi
}
output_message "INFO" "Iniciando el proceso de ETL VÍAS DE CIRCULACIÓN"
output_message "INFO" "Verificando paquetes requeridos"
check_library "$(ogr2ogr --version)"
check_library "$(psql --version)"
check_library "$(wget --version | grep 'GNU Wget')"
check_library "$(unzip -v | grep Info-ZIP)"

output_message "INFO" "Verificando parámetros"
if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
then
 if ogrinfo "PG:host=${POSTGRES_HOST} dbname=${POSTGRES_DBNAME} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" | grep "successful"
 then
  URL='https://geoservicios.indec.gov.ar/geoserver/sig/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=sig:vias&outputFormat=SHAPE-ZIP'
  FILE='vias.zip'

  output_message "INFO" "Verificando disponibilidad del archivo remoto ${FILE}"
  if [[ `wget -S --spider ${URL}  2>&1 | grep 'HTTP/1.1 200 OK'` ]];
  then
      output_message "INFO" "Descargando ${FILE} de ${URL}"
      wget -c --progress=dot -e dotbytes=1M -O ${FILE} ${URL} --no-check-certificate
      unzip ${FILE} -d vias

      output_message "INFO" "Importando ${FILE}"
      export SHAPE_ENCODING="LATIN1"
      ogr2ogr -overwrite -progress -f "PostgreSQL" \
         PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
         vias/vias.shp -nln indec_vias -nlt MULTILINESTRING -lco GEOMETRY_NAME=geom

      mkdir -p logs
      output_message "INFO" "$(ogrinfo -ro -so vias/vias.shp -al)"
      rm ${FILE}; rm -rf vias;
      output_message "INFO" "El proceso de ETL VÍAS DE CIRCULACIÓN finalizó exitosamente"
  else
      output_message "WARNING" "El fichero ${FILE} remoto no existe o no se pudo establecer la conexión"
  fi
  else
    output_message "ERROR" "No se pudo establecer la conexión"
    exit 1
  fi
else
  output_message "WARNING" "Verifique los parámetros de conexión"
  exit 1
fi
