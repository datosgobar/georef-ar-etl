# -*- coding: utf-8 -*-

from search.models import *
import json
import psycopg2
import requests


class PostgresWrapper:
    def search_address(self, address):
        connection = psycopg2.connect(
            dbname="georef", user="postgres", password="postgres")
        with connection.cursor() as cursor:
            query = "SELECT tipo_camino || ' ' || nombre_completo || ', ' \
                    || localidad || ', ' || provincia AS addr \
                    FROM nombre_calles \
                    WHERE nombre_completo ILIKE '%(address)s%%' \
                    OR localidad ILIKE '%(address)s%%' \
                    LIMIT 10" % {'address': address}
            cursor.execute(query)
            results = cursor.fetchall()
        
        return json.dumps([row[0] for row in results])
