# Makefile para georef-ar-etl

ETL_PYTHON ?= python
ETL_PIP ?= pip3
ETL_ALEMBIC ?= alembic

ALEMBIC_COMMAND = $(ETL_ALEMBIC) --config config/alembic.ini
ETL_COMMAND = $(ETL_PYTHON) -m georef_ar_etl
GIT_BRANCH ?= master

.PHONY: files

# Ejecución del ETL

update:
	git checkout $(GIT_BRANCH)
	git pull origin $(GIT_BRANCH)
	make migrate
	$(ETL_PIP) install -r requirements.txt -r requirements-dev.txt

# Ejecuta todos los procesos
run:
	$(ETL_COMMAND)

# Ejecuta el proceso de provincias
provincias:
	$(ETL_COMMAND) -p provincias

# Ejecuta el proceso de departamentos
departamentos:
	$(ETL_COMMAND) -p departamentos

# Ejecuta el proceso de gobiernos_locales
gobiernos_locales:
	$(ETL_COMMAND) -p gobiernos_locales

# Ejecuta el proceso de localidades_censales
localidades_censales:
	$(ETL_COMMAND) -p localidades_censales

# Ejecuta el proceso de calles
calles:
	$(ETL_COMMAND) -p calles

# Ejecuta todos los procesos, pero solo la parte de generación de archivos
files:
	$(ETL_COMMAND) -p provincias --start 8 --no-mail
	$(ETL_COMMAND) -p departamentos --start 9 --no-mail
	$(ETL_COMMAND) -p gobiernos_locales --start 9 --no-mail
	$(ETL_COMMAND) -p localidades_censales --start 9 --no-mail
	$(ETL_COMMAND) -p asentamientos --start 8 --no-mail
	$(ETL_COMMAND) -p localidades --start 6 --no-mail
	$(ETL_COMMAND) -p calles --start 5 --no-mail
	$(ETL_COMMAND) -p intersecciones --start 4 --no-mail
	$(ETL_COMMAND) -p cuadras --start 6 --no-mail
	$(ETL_COMMAND) -p sinonimos -p terminos_excluyentes --no-mail

info:
	$(ETL_COMMAND) -c info

console:
	$(ETL_COMMAND) -c console

stats:
	$(ETL_COMMAND) -c stats


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

# TEST_FILES puede definirse opcionalmente
test:
	python -m unittest $(TEST_FILES)

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
	$(ETL_COMMAND) -p gobiernos_locales --end 4
	$(ETL_COMMAND) -p localidades_censales --end 4
	$(ETL_COMMAND) -p asentamientos --end 4
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
		tests/test_files/test_gobiernos_locales \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_gobiernos_locales where in1 like '$(TEST_PROVINCE)%'" \
		-nln "test_gobiernos_locales" \
		-lco "ENCODING=utf-8" \
		-overwrite

	ogr2ogr -f "ESRI Shapefile" \
		tests/test_files/test_asentamientos \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_asentamientos where cod_bahra like '$(TEST_PROVINCE)%'" \
		-nln "test_asentamientos" \
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
