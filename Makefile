ENVIROMENTS=. $(PROJECT_DIR)/environment.sh
MANAGE?=$(PROJECT_DIR)/manage.py
PIP?=$(VENV_DIR)/bin/pip
PROJECT_DIR=$(shell pwd)
PYTHON?=$(VENV_DIR)/bin/python
RUNSCRIPT=$(MANAGE) runscript
VENV_DIR?=$(PROJECT_DIR)/venv

SHELL := /bin/bash

.PHONY: all

all: virtualenv \
 	 install \
 	 migrate_db \
 	 etl_entities \
 	 etl_roads \
 	 load_entities \
 	 load_roads \
 	 create_entities_data \
 	 create_roads_data

virtualenv:
	rm -rf venv
	virtualenv -p python3.6 $(VENV_DIR)

install: requirements

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
