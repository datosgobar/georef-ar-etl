#!/usr/bin/env bash

set -o errexit
set -o nounset

function output_message (){
    echo "$(date '+%H:%M:%S') | $1 | ${USER} | etl_ign_entidades | $2" >> logs/etl_$(date '+%Y%m%d').log
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

output_message "INFO" "Iniciando el proceso de ETL ENTIDADES"
output_message "INFO" "Verificando paquetes requeridos"
check_library "$(ogr2ogr --version)"
check_library "$(psql --version)"
check_library "$(wget --version | grep 'GNU Wget')"
check_library "$(unzip -v | grep Info-ZIP)"

output_message "INFO" "Verificando parámetros"
if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
then
 output_message "INFO" "Verificando conexión"
 if ogrinfo "PG:host=${POSTGRES_HOST} dbname=${POSTGRES_DBNAME} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" | grep "successful"
 then
  output_message "INFO" "Conexión establecida"
  declare -a ENTITIES=("provincia" "departamento" "municipio")
  declare URL_PORTAL='http://www.ign.gob.ar/descargas/geodatos/'
  declare result=true

  for i in "${ENTITIES[@]}"
  do
    declare FILE="${i}.zip"
    declare URL="${URL_PORTAL}${FILE}"

    output_message "INFO" "Verificando disponibilidad del archivo remoto ${FILE}"
    if [[ `wget -S --spider ${URL} 2>&1 | grep 'HTTP/1.1 200 OK'` ]]
    then
        output_message "INFO" "Descargando ${FILE} de ${URL_PORTAL}"
        wget -c --progress=dot -e dotbytes=1M -O ${FILE} ${URL}
        unzip ${FILE} -d ${i}

        output_message "INFO" "Importando ${FILE}"
        ogr2ogr -overwrite -progress -f "PostgreSQL" \
        PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
        "${i}/${i}.shp" -nln "ign_${i}s_tmp" -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom

        mkdir -p logs
        output_message "INFO" "$(ogrinfo -ro -so "${i}/${i}.shp" -al)"
        rm ${FILE}; rm -rf ${i}
    else
        result=false
        output_message "WARNING" "El fichero ${FILE} remoto no existe o no se pudo establecer la conexión"
    fi
  done

  declare URL_SHP_BARHA='http://wms.ign.gob.ar/bahra/descargas/BAHRA_2014_version_1.1_shape.tar.gz'
  declare FILE_BARHA='bahra.tar.gz'

  output_message "INFO" "Verificando disponibilidad del archivo remoto ${FILE_BARHA}"
  if [[ `wget -S --spider ${URL_SHP_BARHA}  2>&1 | grep 'HTTP/1.1 200 OK'` ]]
  then
      output_message "INFO" "Descargando ${FILE_BARHA} de ${URL_SHP_BARHA}"
      wget -c --progress=dot -e dotbytes=1M -O ${FILE_BARHA} ${URL_SHP_BARHA}
      mkdir -p bahra
      tar -zxvf ${FILE_BARHA} -C bahra

      output_message "INFO" "Importando ${FILE_BARHA}"
      export SHAPE_ENCODING="LATIN1"
      ogr2ogr -overwrite -progress -f "PostgreSQL" \
      PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
      bahra/bahra_27112014.shp -nln ign_bahra_tmp -nlt MULTIPOINT -lco GEOMETRY_NAME=geom -lco precision=NO

      output_message "INFO" "$(ogrinfo -ro -so bahra/bahra_27112014.shp -al)"
      rm ${FILE_BARHA}; rm -rf bahra
  else
      result=false
      output_message "WARNING" "El fichero ${FILE_BARHA} remoto no existe o no se pudo establecer la conexión"
  fi
  else
    result=false
    output_message "ERROR" "No se pudo establecer la conexión"
    exit 1
  fi
  else
    result=false
    output_message "WARNING" "Verifique los parámetros de conexión"
    exit 1
fi

if [ "$result" = true ]; then
     output_message "INFO" "El proceso de ETL ENTIDADES finalizó exitosamente"
fi
