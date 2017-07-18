from django.db import models


SOURCES = (
    ('georef', 'Georef'),
    ('here', 'HERE'),
    ('osm', 'OpenStreetMap')
)


class AddressType(models.Model):
    name = models.CharField(max_length=50)  # 'house_number', 'street', etc.

    def __str__(self):
        return "{}".format(self.name)


class City(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return "{}".format(self.name)

    class Meta:
        verbose_name_plural = 'Cities'


class State(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return "{}".format(self.name)


class Address(models.Model):
    search_text = models.CharField(max_length=250, null=True)
    name = models.CharField(max_length=250)
    street = models.CharField(max_length=100, blank=True, null=True)
    house_number = models.IntegerField(blank=True, null=True)
    lat = models.DecimalField(max_digits=1000, decimal_places=10)
    lon = models.DecimalField(max_digits=1000, decimal_places=10)
    type = models.ForeignKey(AddressType)
    district = models.CharField(max_length=100, blank=True, null=True)
    suburb = models.CharField(max_length=100, blank=True, null=True)
    city = models.ForeignKey(City, blank=True, null=True)
    state = models.ForeignKey(State, blank=True, null=True)
    postal_code = models.CharField(max_length=8, blank=True, null=True)
    source = models.CharField(max_length=4, choices=SOURCES)

    def __str__(self):
        return '{} {}'.format(self.name, self.house_number)

    class Meta:
        verbose_name_plural = 'Addresses'
