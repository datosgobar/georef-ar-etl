# -*- coding: utf-8 -*-

"""Tests de la aplicación Search."""

from django.test import TestCase
from search.wrappers import HereWrapper


class HereAPITest(TestCase):
    """Tests para la interfaz con API de HERE."""

    def setUp(self):
        self.url = 'https://geocoder.cit.api.here.com/6.2/geocode.json'
        self.search_text = 'Avenida Presidente Roque Sáenz Peña 788'
        self.app_code = '2KEvGLCDtqH7HLUTNxDu3A&gen=8'
        self.app_id = 'Qop1chzvUyZdOZJatmeG'

    def test_search_address(self):
        wrapper = HereWrapper(self.url, self.app_code, self.app_id)
        response = wrapper.search_address(self.search_text)
        self.assertTrue(response.ok)
