# Makefile para georef-ar-etl

ETL_PYTHON ?= python
ETL_PIP ?= pip3
ETL_ALEMBIC ?= alembic

ALEMBIC_COMMAND = $(ETL_ALEMBIC) --config config/alembic.ini
ETL_COMMAND = $(ETL_PYTHON) -m georef_ar_etl
GIT_BRANCH ?= master

TEST_FILES ?= *.py

# Ejecución del ETL

update:
	git checkout $(GIT_BRANCH)
	git pull origin $(GIT_BRANCH)
	make migrate
	$(ETL_PIP) install -r requirements.txt -r requirements-dev.txt

run:
	$(ETL_COMMAND)

info:
	$(ETL_COMMAND) -c info

console:
	$(ETL_COMMAND) -c console


# Recetas para utilizar Alembic, la herramienta de migraciones de bases de
# datos para SQLAlchemy.

# Para generar una nueva migración (solo desarrolladores):
# 1) Modificar models.py con los datos deseados.
# 2) Borrar todas las tablas del proyecto.
# 3) Ejecutar make migrate.
# 4) Ejecutar make makemigrations.
# 5) Editar la migración creada para agregarle título.

makemigrations:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) revision --autogenerate

migrate:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) upgrade head

history:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) history


# Recetas para uso de desarrollo del ETL:

# Testing/Linting

code_checks:
	pyflakes georef_ar_etl tests
	pylint georef_ar_etl tests

test:
	python -m unittest

test_custom:
	python -m unittest tests/$(TEST_FILES)

coverage:
	coverage run --source=georef_ar_etl --omit=georef_ar_etl/__main__.py -m unittest
	coverage report

# Crear archivos de prueba utilizando una sola provincia como dato.
# Para lograr esto, se ejecuta cada proceso del ETL hasta el punto donde se
# cargó la tabla tmp_X, directamente de los datos descargados. Desde esa tabla,
# se genera un archivo Shapefile y se lo guarda en tests/test_files/.
# Se debería re-ejecutar la receta cada vez que cambia el esquema de datos utilizado
# por IGN/INDEC/etc. en sus archivos, y adaptar los tests correspondientes.
TEST_PROVINCE = 70
create_test_files:
	$(ETL_COMMAND) -p provincias --end 3
	$(ETL_COMMAND) -p departamentos --end 4
	$(ETL_COMMAND) -p municipios --end 4
	$(ETL_COMMAND) -p localidades --end 4
	$(ETL_COMMAND) -p localidades_censales --end 4
	$(ETL_COMMAND) -p calles --end 3

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_provincias \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_provincias where in1='$(TEST_PROVINCE)'" \
		-nln "test_provincias" \
		-lco "ENCODING=utf-8" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_departamentos \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_departamentos where in1 like '$(TEST_PROVINCE)%'" \
		-nln "test_departamentos" \
		-lco "ENCODING=utf-8" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_municipios \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_municipios where in1 like '$(TEST_PROVINCE)%'" \
		-nln "test_municipios" \
		-lco "ENCODING=utf-8" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_localidades \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_localidades where cod_bahra like '$(TEST_PROVINCE)%'" \
		-nln "test_localidades" \
		-lco "ENCODING=utf-8" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_localidades_censales \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_localidades_censales where link like '$(TEST_PROVINCE)%'" \
		-nln "test_localidades_censales" \
		-lco "ENCODING=utf-8" \
		-t_srs "EPSG:4326" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_cuadras \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_cuadras where nomencla like '$(TEST_PROVINCE)%'" \
		-nln "test_cuadras" \
		-lco "ENCODING=utf-8" \
		-overwrite
