ENVIRONMENTS=. $(PROJECT_DIR)/environment.sh
MANAGE?=$(PROJECT_DIR)/manage.py
PIP?=$(VENV_DIR)/bin/pip
PROJECT_DIR=$(shell pwd)
PYTHON?=$(VENV_DIR)/bin/python3.6
RUNSCRIPT=$(MANAGE) runscript
VENV_DIR?=$(PROJECT_DIR)/venv
GIT_BRANCH?=master

SHELL := /bin/bash

.PHONY: all

.DEFAULT: help

help:
	@echo "make all			Instala georef-ar-etl, carga y genera los datos de entidades y vías de circulación"
	@echo "make install    		Crea entorno virtual e instala dependencias con pip"
	@echo "make update     		Sincroniza y carga la base de datos con entidades y vías de circulación"
	@echo "make create_data 		Genera datos de entidades y vías de circulación en formato Json"
	@echo "make install_cron		Instala crontab para sincronizar y cargar la base de datos con entidades y vías de circulación, y generar datos"

all: install \
 update \
 create_data

install: virtualenv \
 requirements

update:	pull \
 migrate_db \
 etl_entities \
 etl_roads \
 create_entities_report \
 load_entities \
 load_roads \
 create_data \
 create_synonyms_file \
 create_excluding_terms_file \
 custom_steps

create_data: create_entities_data \
 create_roads_data

pull:
	git pull origin $(GIT_BRANCH)
	git fetch --tags

virtualenv:
	rm -rf venv
	python3.6 -m venv $(VENV_DIR)

requirements:
	$(PIP) install -r $(PROJECT_DIR)/requirements.txt

migrate_db:
	$(ENVIRONMENTS) && $(PYTHON) $(MANAGE) migrate

etl_entities:
	$(ENVIRONMENTS) && $(PROJECT_DIR)/etl_scripts/etl_ign_entidades.sh

etl_roads:
	$(ENVIRONMENTS) && $(PROJECT_DIR)/etl_scripts/etl_indec_vias.sh

load_entities:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) load_entities

load_roads:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) load_roads

load_intersections:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) load_intersections

create_entities_data:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) create_entities_data

create_roads_data:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) create_roads_data

create_intersections_data:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) create_intersections_data

create_entities_report:
	$(ENVIRONMENTS) && $(PYTHON) $(RUNSCRIPT) create_entities_report

# TODO: Una vez que el proceso de generación del archivo de sinónimos
# esté decidido, modificar la receta para que el proceso deposite el
# archivo generado en data/latest/.
create_synonyms_file:
	cp georef/sinonimos-nombres.txt data/latest/

# TODO: Seguir mismo procedimiento que en 'create_synonyms_file'.
create_excluding_terms_file:
	cp georef/terminos-excluyentes-nombres.txt data/latest/

install_cron: config/cron
	@echo "GEOREF_ETL_DIR=$(PROJECT_DIR)" >> .cronfile
	cat config/cron >> .cronfile
	crontab .cronfile
	rm .cronfile

custom_steps:
	if [[ -f scripts/custom_steps.sh ]]; then \
		bash scripts/custom_steps.sh; \
	fi;
