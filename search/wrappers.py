# -*- coding: utf-8 -*-

from search.models import *
from georef.settings import API_URL
import urllib.request


class APIWrapper:
    def search_address(self, address):
        api_url = API_URL + 'normalizador?direccion='
        return urllib.request.urlopen(api_url + address).read()
