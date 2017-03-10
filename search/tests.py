# -*- coding: utf-8 -*-

"""Tests de la aplicación Search."""

from django.test import TestCase
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


class NominatimAPITest(TestCase):
    """Tests para la interfaz con API de Nominatim."""

    def setUp(self):
        self.url = 'http://nominatim.openstreetmap.org'
        self.format = 'json'
        self.polygon = '1'
        self.address_details = '1'

    def test_address_with_match(self):
        wrapper = NominatimWrapper(self.url, self.format, self.polygon, self.address_details)
        address = 'Avenida Roque Sáenz Peña 788, Buenos Aires'
        response = wrapper.search_address(address)
        self.assertIsNot(response, None)
