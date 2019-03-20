ALEMBIC_COMMAND = alembic --config config/alembic.ini

# Recetas para utilizar Alembic, la herramienta de migraciones de bases de
# datos para SQLAlchemy.

makemigrations:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) revision --autogenerate

migrate:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) upgrade head

history:
	PYTHONPATH=$$(pwd) $(ALEMBIC_COMMAND) history

# Testing/Linting

code_checks:
	pyflakes georef_ar_etl tests
	pylint georef_ar_etl tests

test:
	python -m unittest


# Desarrollo

# Crear archivos de prueba utilizando la provincia de Santa Fe como dato
create_test_files:
	ogr2ogr \
		-f "ESRI Shapefile" \
		tests/test_files/test_provincias \
		"PG:host=$$DB_HOST dbname=$$DB_NAME user=$$DB_USER password=$$DB_PASS" \
		-sql "select * from tmp_provincias where in1='82'" \
		-nln "test_provincias"
