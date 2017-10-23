# -*- coding: utf-8 -*-
from collections import defaultdict
from django.db import connections
from geo_admin.models import Consumer
import base64
import jwt


def get_consumer(username):
    try:
        consumer = Consumer.objects.get(user__username=username)
        return consumer
    except Consumer.DoesNotExist:
        return None


def get_token_for(consumer):
    if consumer is not None:
        return jwt.encode({'iss': consumer.api_key},
                          base64.b64decode(consumer.api_secret),
                          algorithm='HS256')
    return 'No se pudo obtener credenciales para el usuario ingresado.'


def get_api_metrics():
    with connections['kong'].cursor() as cursor:
        query = ('SELECT C.username, A.name, SUM(R.value) '
                 'FROM apis A, consumers C, ratelimiting_metrics R '
                 'WHERE A.id = R.api_id AND C.id::text = R.identifier '
                 'GROUP BY C.username, A.name '
                 'ORDER BY 1, 3 desc;')
        cursor.execute(query)
        result = cursor.fetchall()

        apis = defaultdict(int)
        users = defaultdict(int)
        for row in result:
            user, api, hits = row
            apis[api] += hits
            users[user] += hits

        metrics = {'apis': dict(apis), 'users': dict(users), 'user_api': result}
        return {'metrics': metrics}
