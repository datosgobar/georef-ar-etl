#!/usr/bin/env bash

set -o errexit
set -o nounset

echo '-- Verificando paquetes requeridos --\n'
if  psql --version; then echo "---"; fi
if  wget --version | grep 'GNU Wget' ; then echo "---"; fi

if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
then
    declare PGPASSWORD=${POSTGRES_PASSWORD}
    declare -a COD_ENTITIES=("02" "06" "10" "14" "18" "22" "26" "30" "34" "38" "42" "46" "50" "54" "58" "62" "66" "70" "74" "78" "82" "86" "90" "94") 
    psql -h ${POSTGRES_HOST} -U ${POSTGRES_USER} -d ${POSTGRES_DBNAME} -c "TRUNCATE TABLE public.indec_cuadras RESTART IDENTITY;"
    for i in "${COD_ENTITIES[@]}"
    do
        declare FILE="${i}.csv"
        declare URL_GEOSERVICIOS="https://geoservicios.indec.gov.ar/geoserver/sig/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=sig:cuadras&outputFormat=csv&CQL_FILTER=nomencla+like+%27${i}%25%27"
        wget --progress=dot -e dotbytes=1M -O ${FILE} ${URL_GEOSERVICIOS} --no-check-certificate
        psql -h ${POSTGRES_HOST} -U ${POSTGRES_USER} -d ${POSTGRES_DBNAME} -c "COPY public.indec_cuadras FROM '${PWD}/${FILE}' DELIMITER ',' CSV HEADER"
    done
    echo "Terminado!"
fi