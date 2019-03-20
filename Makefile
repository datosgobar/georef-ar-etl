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
