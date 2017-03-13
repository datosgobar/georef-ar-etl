from django.db import models

SOURCES = (
    ('here', 'HERE'),
    ('osm', 'OpenStreetMap')
)


class Address(models.Model):
    name = models.CharField(max_length=250)
    house_number = models.IntegerField(blank=True, null=True)
    #geom = models.PointField(blank=True, null=True)
    type = models.ForeignKey(AddressType)
    district = models.CharField(max_length=100, blank=True, null=True)
    suburb = models.CharField(max_length=100, blank=True, null=True)
    city = models.ForeignKey(City)
    state = models.ForeignKey(State)
    postal_code = models.CharField(max_length=8, blank=True, null=True)
    source = models.CharField(max_length=4, choices=SOURCES)


class AddressType(models.Model):
    name = models.CharField(max_length=50)  # 'house_number', 'street', etc.


class City(models.Model):
    name = models.CharField(max_length=100)


class State(models.Model):
    name = models.CharField(max_length=100)
