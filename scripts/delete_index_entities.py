# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, ElasticsearchException


def run():
    try:
        indices = ['provincias', 'departamentos', 'municipios',
                   'localidades', 'bahra']
        for index in indices:
            Elasticsearch().indices.delete(index=index)
            print('Se eliminó el índice "%s".' % index)
    except (ElasticsearchException, SyntaxError) as error:
        print(error)
