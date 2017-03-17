# -*- coding: utf-8 -*-

"""Tests de la aplicación Search."""

from django.test import TestCase
from search.models import *
from search.wrappers import HereWrapper, NominatimWrapper


class HereAPITest(TestCase):
    """Tests para la interfaz con API de HERE."""

    def setUp(self):
        self.url = 'https://geocoder.cit.api.here.com/6.2/geocode.json'
        self.app_code = '2KEvGLCDtqH7HLUTNxDu3A&gen=8'
        self.app_id = 'Qop1chzvUyZdOZJatmeG'

    def test_address_with_match(self):
        wrapper = HereWrapper(self.url, self.app_code, self.app_id)
        address = 'Avenida Roque Sáenz Peña 788, Buenos Aires'
        response = wrapper.search_address(address)
        self.assertTrue('houseNumber' in response[0]['MatchLevel'])

    def test_address_without_match(self):
        wrapper = HereWrapper(self.url, self.app_code, self.app_id)
        address = 'Avenida Roque Sáenz Peña 788'
        response = wrapper.search_address(address)
        self.assertIs(response, None)

    def test_convert_result_to_address_model(self):
        wrapper = HereWrapper(self.url, self.app_code, self.app_id)
        result = {
            'Relevance': 1.0,
            'MatchLevel': 'houseNumber',
            'MatchQuality': {
                'Country': 1.0,
                'City': 1.0,
                'Street': [1.0],
                'HouseNumber': 1.0},
            'MatchType': 'interpolated',
            'Location': {
                'LocationId': 'NT_lWBq3p.Bo6htVj-45UZJ9C_3gDO',
                'LocationType': 'address',
                'DisplayPosition': {
                    'Latitude': -34.606124,
                    'Longitude': -58.3773523},
                'Address': {
                    'Label': 'Avenida Presidente Roque Sáenz Peña 788',
                    'Country': 'ARG',
                    'State': 'Ciudad Autónoma de Buenos Aires',
                    'County': 'Ciudad de Buenos Aires',
                    'City': 'Ciudad de Buenos Aires',
                    'District': 'San Nicolás',
                    'Street': 'Avenida Presidente Roque Sáenz Peña',
                    'HouseNumber': '788',
                    'PostalCode': '1035'}
                }
            }
        model = wrapper.get_address_from(result)
        model.save()
        model = Address.objects.all()[0]
        self.assertIsNotNone(model.id)


class NominatimAPITest(TestCase):
    """Tests para la interfaz con API de Nominatim."""

    def setUp(self):
        self.url = 'http://nominatim.openstreetmap.org'
        self.format = 'json'
        self.country_code = 'ar'
        self.address_details = '1'

    def test_address_with_match(self):
        wrapper = NominatimWrapper(
            self.url, self.format, self.country_code, self.address_details)
        address = 'Avenida Roque Sáenz Peña 788, Buenos Aires'
        response = wrapper.search_address(address)
        self.assertIsNot(response, None)

    def test_convert_result_to_address_model(self):
        wrapper = NominatimWrapper(
            self.url, self.format, self.country_code, self.address_details)
        result = {
            "place_id": "18610386",
            "licence": "Data © OpenStreetMap contributors",
            "osm_type": "way",
            "osm_id": "208271943",
            "boundingbox": [
                "-34.605326526531",
                "-34.605226526531",
                "-58.376511089796",
                "-58.376411089796"
            ],
            "lat": "-34.6052765265306",
            "lon": "-58.3764610897959",
            "display_name": "255, Maipú, Microcentro, San Nicolás, Argentina",
            "class": "place",
            "type": "house",
            "importance": 0.611,
            "address": {
                "house_number": "255",
                "road": "Maipú",
                "neighbourhood": "Microcentro",
                "suburb": "San Nicolás",
                "city": "Buenos Aires",
                "state_district": "Comuna 1",
                "state": "Ciudad Autónoma de Buenos Aires",
                "postcode": "C1006",
                "country": "Argentina",
                "country_code": "ar"
            }
        }
        model = wrapper.get_address_from(result)
        model.save()
        model = Address.objects.all()[0]
        self.assertIsNotNone(model.id)

