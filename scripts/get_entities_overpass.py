# -*- coding: utf-8 -*-

import requests
import json


def run():
    try:
        url = 'http://overpass-api.de/api/interpreter'
        endpoints = [[4, 'states'], [5, 'departments'], [8, 'localities']]

        for key, value in endpoints:
            entities = []
            query = '[out:json][timeout:320];area[name="Argentina"];' \
                    'rel[boundary=administrative]%s' \
                    '[admin_level=%s](area);out center;' \
                    % (('["ISO3166-2"~"AR"]' if key == 4 else ''), key)

            print(f'-- Extrayendo datos de la entidad {value}. --')
            result = requests.get(url, data=query).json()

            for entity in result['elements']:
                entities.append(entity['tags'])
            with open('data/osm_%s.json' % value, 'w', encoding='utf-8') as f:
                f.write(json.dumps(entities, ensure_ascii=False))
                print(f'-- Entidad {value} extraída exitosamente. --')
    except Exception as e:
        print(e)
