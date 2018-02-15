#!/usr/bin/env bash

set -o errexit
set -o nounset

echo '-- Verificando paquetes requeridos --\n'
if  ogr2ogr --version; then echo "---"; fi
if  psql --version; then echo "---"; fi
if  wget --version | grep 'GNU Wget' ; then echo "---"; fi
if  unzip -v | grep Info-ZIP ; then echo "---"; else exit 1; fi

if [ -n ${POSTGRES_HOST} ] && [ -n ${POSTGRES_USER} ] && [ -n ${POSTGRES_DBNAME} ] && [ -n ${POSTGRES_PASSWORD} ]
then
 if ogrinfo "PG:host=${POSTGRES_HOST} dbname=${POSTGRES_DBNAME} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" | grep "successful"
 then
  declare -a ENTITIES=("provincia" "departamento" "municipio")
  declare URL_PORTAL='http://www.ign.gob.ar/descargas/geodatos/'

  for i in "${ENTITIES[@]}"
  do
    declare FILE="${i}.zip"
    declare URL="${URL_PORTAL}${FILE}"

    wget --progress=dot -e dotbytes=1M -O ${FILE} ${URL}
    unzip ${FILE} -d ${i}

    ogr2ogr -overwrite -progress -f "PostgreSQL" \
    PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
    "${i}/${i}.shp" -nln "ign_${i}s" -nlt POLYGON -lco GEOMETRY_NAME=geom

    echo "--------------------------------------------------------------------------- $(date)" >> ign.log
    ogrinfo -ro -so "${i}/${i}.shp" -al >> ign.log
  done

  declare URL_SHP_BARHA='http://wms.ign.gob.ar/bahra/descargas/BAHRA_2014_version_1.1_shape.tar.gz'
  declare FILE_BARHA='bahra.tar.gz'

  wget --progress=dot -e dotbytes=1M -O ${FILE_BARHA} ${URL_SHP_BARHA}
  mkdir -p bahra
  tar -zxvf ${FILE_BARHA} -C bahra

  export SHAPE_ENCODING="LATIN1"

  ogr2ogr -overwrite -progress -f "PostgreSQL" \
  PG:"host=${POSTGRES_HOST} user=${POSTGRES_USER} dbname=${POSTGRES_DBNAME} password=${POSTGRES_PASSWORD}" \
  bahra/bahra_27112014.shp -nln ign_bahra -nlt MULTIPOINT -lco GEOMETRY_NAME=geom -lco precision=NO

  echo "--------------------------------------------------------------------------- $(date)" >> ign.log
  ogrinfo -ro -so "bahra/bahra_27112014.shp" -al >> ign.log

  echo "Terminado!"
  fi
fi