ENVIROMENTS=. $(PROJECT_DIR)/environment.sh
MANAGE?=$(PROJECT_DIR)/manage.py
PIP?=$(VENV_DIR)/bin/pip
PROJECT_DIR=$(shell pwd)
PYTHON?=$(VENV_DIR)/bin/python
RUNSCRIPT=$(MANAGE) runscript
VENV_DIR?=$(PROJECT_DIR)/venv

SHELL := /bin/bash

.PHONY: all

.DEFAULT: help

help:
	@echo "make all			Instala Georef Etl, actualiza y genera los datos de entidades y vías de circulación "
	@echo "make install    		Crea entorno virtual e instala dependencias con pip"
	@echo "make update     		Sincroniza y carga la base de datos con entidades y vías de circulación"
	@echo "make create_data 		Genera datos de entidades y vías de circulación en formato Json"

all: install \
 	 update \
 	 create_data

install: virtualenv \
		 requirements

update: migrate_db \
		etl_entities \
		etl_roads \
		load_entities \
		load_roads

create_data: create_entities_data \
 	  create_roads_data

virtualenv:
	rm -rf venv
	virtualenv -p python3.6 $(VENV_DIR)

requirements:
	$(PIP) install -r $(PROJECT_DIR)/requirements.txt --no-warn-script-location

migrate_db:
	$(ENVIROMENTS) && $(PYTHON) $(MANAGE) migrate

etl_entities:
	$(ENVIROMENTS) && $(PROJECT_DIR)/etl_scripts/etl_ign_entidades.sh

etl_roads:
	$(ENVIROMENTS) && $(PROJECT_DIR)/etl_scripts/etl_indec_vias.sh

load_entities:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) load_entities

load_roads:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) load_roads

create_entities_data:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_entities_data

create_roads_data:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_roads_data
