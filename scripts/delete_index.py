# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, ElasticsearchException


def run(*args):
    try:
        if not args:
            raise SyntaxError('Error: debe ingresar uno o más índices.')
        for name in args:
            Elasticsearch().indices.delete(index=name)
            print('Se eliminó el índice "%s".' % name)
    except (ElasticsearchException, SyntaxError) as error:
        print(error)
