# -*- coding: utf-8 -*-
from benchmark.models import *
from georef.settings import API_URL, KONG_URL, HERE, OSM_API_URL
from urllib import parse, request
import requests

class GeorefWrapper:
    """Interfaz para la API REST de Georef."""
    def search_address(self, address, locality=None, state=None):
        resource = API_URL + 'direcciones?'
        query = { 'direccion': address }
        if locality:
            query.update(localidad=locality)
        if state:
            query.update(provincia=state)
        resource += parse.urlencode(query)
        return request.urlopen(resource).read()
    
    def get_address_from(self, search_text, result):
        """Procesa una dirección de Georef y retorna un objeto Address."""
        type = AddressType.objects.get_or_create(name=result['tipo'])[0]
        address = Address(
            search_text=search_text,
            name=result['nomenclatura'],
            street=result['nombre'],
            house_number=result.get('altura'),
            lat=result.get('ubicacion', {}).get('lat') or 0,
            lon=result.get('ubicacion', {}).get('lon') or 0,
            type=type,
            source='georef'
        )
        if 'localidad' in result:
            address.city = City.objects.get_or_create(name=result['localidad'])[0]
        if 'provincia' in result:
            address.state = State.objects.get_or_create(name=result['provincia'])[0]
        return address
    
    def get_states(self):
        resource = API_URL + 'provincias'
        return request.urlopen(resource).read()

    def get_localities(self):
        resource = API_URL + 'localidades?max=4000'
        return request.urlopen(resource).read()


class HereWrapper:
    """Interfaz para la API REST de HERE."""
    def __init__(self):
        self.url = HERE['API_URL']
        self.app_code = HERE['APP_CODE']
        self.app_id = HERE['APP_ID']

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
    def __init__(self):
        self.url = OSM_API_URL
        self.format = 'json'
        self.country_code = 'ar'
        self.address_details = 1

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
