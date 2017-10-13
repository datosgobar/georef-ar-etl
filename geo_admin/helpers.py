# -*- coding: utf-8 -*-
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
