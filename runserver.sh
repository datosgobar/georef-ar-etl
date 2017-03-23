#!/bin/bash

. secrets.sh

$PYTHON manage.py runserver 0.0.0.0:80
