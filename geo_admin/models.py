from django.db import models


class State(models.Model):
    code = models.CharField(max_length=2, blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)


class Department(models.Model):
    code = models.CharField(max_length=5, blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    state = models.ForeignKey(State, blank=True, null=True)


class Locality(models.Model):
    code = models.CharField(max_length=8, blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    agglomerate = models.CharField(max_length=4, blank=True, null=True)
    department = models.ForeignKey(Department, blank=True, null=True)
    state = models.ForeignKey(State, blank=True, null=True)

    class Meta:
        verbose_name_plural = 'Localities'


class Road(models.Model):
    code = models.CharField(max_length=13)
    name = models.CharField(max_length=5)
    road_type = models.CharField(max_length=25, blank=True, null=True)
    start_left = models.IntegerField(blank=True, null=True)
    start_right = models.IntegerField(blank=True, null=True)
    end_left = models.IntegerField(blank=True, null=True)
    end_right = models.IntegerField(blank=True, null=True)
    geom = models.CharField(max_length=100, blank=True, null=True)
    postal_code = models.CharField(max_length=8, blank=True, null=True)
    locality = models.ForeignKey(Locality, blank=True, null=True)
    state = models.ForeignKey(State, blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
