# -*- coding: utf-8 -*-

import requests


class HereWrapper:
    """Interfaz para la API REST de HERE."""

    def __init__(self, url, app_code, app_id):
        self.url = url
        self.app_code = app_code
        self.app_id = app_id

    def search_address(self, address):
        query = '{}?searchtext={}&app_code={}&app_id={}'.format(
            self.url, address, self.app_code, self.app_id)
        response = requests.get(query)
        return response if response.ok else False
