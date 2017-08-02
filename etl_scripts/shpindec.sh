#!/bin/sh

# Servicio indec
url='http://geoservicios.indec.gov.ar/geoserver/sig/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=sig:vias&outputFormat=SHAPE-ZIP'
file='vias.zip'

# Descargando el shafile
wget --progress=dot -e dotbytes=1M -O vias.zip $url

# Descomprimiendo
unzip vias.zip -d vias

# Cargar shape a PostgreSQL
# Parametros de conexion
host=localhost
user=postgres
dbname=test
pass=postgres

# Importacion
ogr2ogr --config PGCLIENTENCODING LATIN1 -append -f "PostgreSQL" PG:"host=$host user=$user dbname=$dbname password=$pass" vias/vias.shp -nln indec_vias -nlt MULTILINESTRING -lco GEOMETRY_NAME=geom

# Genero un archivo log
echo "--------------------------------------------------------------------------- $(date)" >> indec.log
ogrinfo -ro -so vias/vias.shp -al >> indec.log

echo "Listo"
