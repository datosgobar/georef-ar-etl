# -*- coding: utf-8 -*-

import json
import os
import requests


def run(*args):
    KONG_URL = os.environ.get('KONG_HOST') + ':8001'
    try:
        if not args:
            raise SyntaxError('Error: debe ingresar uno o más nombres de usuario.')

        consumers_url = KONG_URL + '/consumers'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        for name in args:
            response = requests.post(consumers_url, data={'username': name})
            if response.status_code == 201:
                print('Se creó el usuario "%s".' % name)
            credentials_url = KONG_URL + '/consumers/%s/jwt' % name
            response = requests.post(credentials_url, headers=headers, data={})
            if response.status_code == 201:
                print('Se crearon credenciales para el usuario "%s".' % name)
    
    except (requests.RequestException, SyntaxError) as error:
        print(error)
