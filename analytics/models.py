from django.db import models
from geo_admin.models import Locality, State


class Search(models.Model):
    text = models.CharField(max_length=250, verbose_name='texto de búsqueda')
    road_name = models.CharField(max_length=100, blank=True, null=True, verbose_name='nombre')
    road_type = models.CharField(max_length=25, blank=True, null=True, verbose_name='tipo de camino')
    door_number = models.IntegerField(blank=True, null=True, verbose_name='altura')
    postal_code = models.CharField(max_length=8, blank=True, null=True, verbose_name='código postal')
    locality = models.ForeignKey(Locality, blank=True, null=True, verbose_name='localidad')
    state = models.ForeignKey(State, blank=True, null=True, verbose_name='provincia')
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'búsqueda'

    def __str__(self):
        return self.text
