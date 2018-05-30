ENVIROMENTS=. $(PROJECT_DIR)/environment.sh
MANAGE?=$(PROJECT_DIR)/manage.py
PIP?=$(VENV_DIR)/bin/pip
PROJECT_DIR=$(shell pwd)
PYTHON?=$(VENV_DIR)/bin/python3.6
RUNSCRIPT=$(MANAGE) runscript
VENV_DIR?=$(PROJECT_DIR)/venv

SHELL := /bin/bash

.PHONY: all

.DEFAULT: help

help:
	@echo "make all			Instala Georef Etl, carga y genera los datos de entidades y vías de circulación"
	@echo "make install    		Crea entorno virtual e instala dependencias con pip"
	@echo "make update     		Sincroniza y carga la base de datos con entidades y vías de circulación"
	@echo "make create_data 		Genera datos de entidades y vías de circulación en formato Json"
	@echo "make install_cron		Instala crontab para sincronizar y cargar la base de datos con entidades y vías de circulación, y generar datos"

all: install \
 	 update \
 	 create_data

install: virtualenv \
		 requirements

update: migrate_db \
		etl_entities \
		etl_roads \
		create_report_entities \
		load_entities \
		load_roads \

create_data: create_entities_data \
 	  create_roads_data

virtualenv:
	rm -rf venv
	python3.6 -m venv $(VENV_DIR)

requirements:
	$(PIP) install -r $(PROJECT_DIR)/requirements.txt

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

load_intersections:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) load_intersections

create_entities_data:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_entities_data

create_roads_data:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_roads_data

create_intersections_data:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_intersections_data

create_report_entities:
	$(ENVIROMENTS) && $(PYTHON) $(RUNSCRIPT) create_report_entities

install_cron: config/cron
	@echo "GEOREF_ETL_DIR=$(PROJECT_DIR)" >> .cronfile
	cat config/cron >> .cronfile
	crontab .cronfile
	rm .cronfile
