# -*- coding: utf-8 -*-

"""Tests de la aplicación Search."""

from django.test import TestCase
from search.wrappers import PostgresWrapper


class PostgresWrapperTest(TestCase):
    def test_search_address(self):
        wrapper = PostgresWrapper()
        address = 'diagonal'
        response = wrapper.search_address(address)
        self.assertTrue('diagonal norte' in response.lower())
