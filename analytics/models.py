from django.db import models
from geo_admin.models import Locality, State


class Search(models.Model):
    text = models.CharField(max_length=250)
    road_name = models.CharField(max_length=100, blank=True, null=True)
    road_type = models.CharField(max_length=25, blank=True, null=True)
    door_number = models.IntegerField(blank=True, null=True)
    postal_code = models.CharField(max_length=8, blank=True, null=True)
    locality = models.ForeignKey(Locality, blank=True, null=True)
    state = models.ForeignKey(State, blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name_plural = 'Searches'

    def __str__(self):
        return self.text
