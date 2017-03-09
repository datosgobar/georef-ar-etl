# -*- coding: utf-8 -*-

"""Tests de la aplicación Search."""

from django.test import TestCase
from search.wrappers import HereWrapper


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
        self.assertIsNot(response, None)

    def test_address_without_match(self):
        wrapper = HereWrapper(self.url, self.app_code, self.app_id)
        address = 'Avenida Roque Sáenz Peña 788'
        response = wrapper.search_address(address)
        self.assertIs(response, None)
