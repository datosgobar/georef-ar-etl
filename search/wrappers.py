# -*- coding: utf-8 -*-

import requests


class HereWrapper:
    """Interfaz para la API REST de HERE."""

    def __init__(self, url, app_code, app_id):
        self.url = url
        self.app_code = app_code
        self.app_id = app_id

    def search_address(self, address):
        """Busca una dirección en el servicio de HERE.

        Returns:
            list: la lista de resultados encontrados,
            o False si no hay resultados. 
        """
        query = '{}?searchtext={}&app_code={}&app_id={}'.format(
            self.url, address, self.app_code, self.app_id)
        response = requests.get(query)
        try:
            response_view = response.json()['Response']['View']
            return response_view[0]['Result'] if response_view else None
        except ValueError:
            return False


class NominatimWrapper:
    """Interfaz para la API REST de Nominatim"""

    def __init__(self, url, format, polygon, address_details):
        self.url = url
        self.format = format
        self.polygon = polygon
        self.address_details = address_details

    def search_address(self, address):
        query = '{}/search?q={}&format={}&polygon={}&addressdetails={}'.format(
            self.url, address, self.format, self.polygon, self.address_details)
        response = requests.get(query)
        try:
            response_view = response.json()
            return response_view if response_view else None
        except ValueError:
            return False
