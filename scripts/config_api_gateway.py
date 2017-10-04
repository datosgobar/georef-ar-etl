# -*- coding: utf-8 -*-

import os
import requests


def run():
    GEOREF_API_URL = os.environ.get('GEOREF_API_URL')
    KONG_URL = os.environ.get('KONG_URL')
    jwt = {'name': 'jwt', 'config.secret_is_base64': 'true'}
    rates = {'name': 'rate-limiting', 'config.minute': 10}
    stats = {'name': 'statsd'}
    resources = ['provincias', 'departamentos', 'localidades', 'calles', 'direcciones']
    try:
        for resource in resources:
            # Registra recurso de la API.
            apis_url = KONG_URL + '/apis'
            data = {
                'name': resource,
                'uris': '/' + resource,
                'upstream_url': GEOREF_API_URL + resource
            }
            requests.post(apis_url, data=data)
            
            # Activa plugin JWT para el recurso.
            plugins_url = KONG_URL + '/apis/%s/plugins' % resource
            requests.post(plugins_url, data=jwt)
            
            # Activa plugins de métricas y coutas.
            consumers = requests.get(KONG_URL + '/consumers').json()
            for consumer in consumers['data']:
                rates['consumer_id'] = consumer['id']
                requests.post(plugins_url, data=rates)
                stats['consumer_id'] = consumer['id']
                requests.post(plugins_url, data=stats)

    except (requests.RequestException) as error:
        print(error)
