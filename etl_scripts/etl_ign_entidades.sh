#!/usr/bin/env bash

set -o errexit
set -o nounset
declare result=true

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

function check_connection(){
    output_message "INFO" "Verificando parámetros"
    if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
        then
            output_message "INFO" "Verificando conexión"
            if ogrinfo "PG:host=${POSTGRES_HOST} dbname=${POSTGRES_DBNAME} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" | grep "successful"
                then
                    output_message "INFO" "Conexión establecida"
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
}

function get_countries(){
    declare URL_SHP_PAIS='http://wms.ign.gob.ar/geoserver/idera/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=idera:pais&outputFormat=SHAPE-ZIP'
    declare FILE_PAIS='pais.zip'
    output_message "INFO" "Verificando disponibilidad del archivo remoto ${FILE_PAIS}"
    if [[ `wget -S --spider ${URL_SHP_PAIS}  2>&1 | grep 'HTTP/1.1 200'` ]]
    then
        output_message "INFO" "Descargando ${FILE_PAIS} de ${URL_SHP_PAIS}"
        wget -c --progress=dot -e dotbytes=1M -O ${FILE_PAIS} ${URL_SHP_PAIS}
        mkdir -p pais
        unzip ${FILE_PAIS} -d pais

        output_message "INFO" "Importando ${FILE_PAIS}"
        export SHAPE_ENCODING="LATIN1"
        ogr2ogr -overwrite -progress -f "PostgreSQL" \
        PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
        pais/paisPolygon.shp -nln ign_pais_tmp -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom

        output_message "INFO" "$(ogrinfo -ro -so pais/paisPolygon.shp -al)"
        rm ${FILE_PAIS}; rm -rf pais
    else
      result=false
      output_message "WARNING" "El fichero ${FILE_PAIS} remoto no existe o no se pudo establecer la conexión"
    fi
}

function get_entities(){
  declare -a ENTITIES=("provincia" "departamento" "municipio")
  declare URL_PORTAL='http://www.ign.gob.ar/descargas/geodatos/'

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

        output_message "INFO" "$(ogrinfo -ro -so "${i}/${i}.shp" -al)"
        rm ${FILE}; rm -rf ${i}
    else
        result=false
        output_message "WARNING" "El fichero ${FILE} remoto no existe o no se pudo establecer la conexión"
    fi
  done

}


function get_bahra(){
  declare URL_SHP_BARHA='http://www.bahra.gob.ar/descargas/BAHRA_2014_version_1.1_shape.tar.gz'
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

}

mkdir -p logs
output_message "INFO" "Iniciando el proceso de ETL ENTIDADES"
output_message "INFO" "Verificando paquetes requeridos"
check_library "$(ogr2ogr --version)"
check_library "$(wget --version | grep 'GNU Wget')"
check_library "$(unzip -v | grep Info-ZIP)"
check_connection
get_entities
get_countries
get_bahra

if [[ "$result" = true ]]; then
     output_message "INFO" "El proceso de ETL ENTIDADES finalizó exitosamente"
fi
