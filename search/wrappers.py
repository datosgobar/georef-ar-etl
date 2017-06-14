# -*- coding: utf-8 -*-
from search.models import *
from georef.settings import API_URL
from urllib import parse, request
import requests


class GeorefWrapper:
    def search_address(self, address, locality=None, state=None):
        resource = API_URL + 'direcciones?'
        query = { 'direccion': address }
        if locality:
            query.update(localidad=locality)
        if state:
            query.update(provincia=state)
        resource += parse.urlencode(query)
        return request.urlopen(resource).read()


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
        query = '{}?searchtext={}&country=ARG&app_code={}&app_id={}'.format(
            self.url, address, self.app_code, self.app_id)
        response = requests.get(query)
        try:
            response_view = response.json()['Response']['View']
            return response_view[0]['Result'] if response_view else None
        except ValueError:
            return None

    def get_address_from(self, search_text, result):
        """Procesa una dirección de HERE y retorna un objeto Address."""
        type = AddressType.objects.get_or_create(name=result['MatchLevel'])[0]
        latitude = result['Location']['DisplayPosition']['Latitude']
        longitude = result['Location']['DisplayPosition']['Longitude']
        result = result['Location']['Address']
        address = Address(
            search_text=search_text,
            name=result['Label'],
            street=result.get('Street'),
            house_number=result.get('HouseNumber'),
            postal_code=result.get('PostalCode'),
            district=result.get('District'),
            lat=latitude,
            lon=longitude,
            type=type,
            source='here'
        )
        if 'City' in result:
            address.city = City.objects.get_or_create(name=result['City'])[0]
        if 'State' in result:
            address.state = State.objects.get_or_create(name=result['State'])[0]
        return address


class NominatimWrapper:
    """Interfaz para la API REST de Nominatim"""
    def __init__(self, url, format, country_code, address_details):
        self.url = url
        self.format = format
        self.country_code = country_code
        self.address_details = address_details

    def search_address(self, address):
        q = '{}/search?q={}&format={}&countrycodes={}&addressdetails={}'.format(
            self.url, address, self.format,
            self.country_code, self.address_details)
        response = requests.get(q)
        try:
            response_view = response.json()
            return response_view if response_view else None
        except ValueError:
            return None

    def get_address_from(self, search_text, result):
        """Procesa una dirección de OSM y retorna un objeto Address."""
        type = AddressType.objects.get_or_create(name=result['type'])[0]
        latitude = result['lat']
        longitude = result['lon']
        name = result['display_name']
        result = result['address']
        address = Address(
            search_text=search_text,
            name=name,
            street=result.get('road'),
            house_number=result.get('house_number'),
            postal_code=result.get('postcode'),
            district=result.get('suburb'),
            lat=latitude,
            lon=longitude,
            type=type,
            source='osm'
        )
        if 'city' in result:
            address.city = City.objects.get_or_create(name=result['city'])[0]
        if 'state' in result:
            address.state = State.objects.get_or_create(name=result['state'])[0]
        return address
