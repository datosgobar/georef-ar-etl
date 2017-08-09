from django.db import models


SOURCES = (
    ('georef', 'Georef'),
    ('here', 'HERE'),
    ('osm', 'OpenStreetMap')
)


class AddressType(models.Model):
    name = models.CharField(max_length=50, verbose_name='nombre')  # 'house_number', 'street', etc.

    class Meta:
        verbose_name = 'tipo de dirección'
        verbose_name_plural = 'tipo de direcciones'
    
    def __str__(self):
        return "{}".format(self.name)


class City(models.Model):
    name = models.CharField(max_length=100, verbose_name='nombre')

    class Meta:
        verbose_name = 'ciudad'
        verbose_name_plural = 'ciudades'

    def __str__(self):
        return "{}".format(self.name)


class State(models.Model):
    name = models.CharField(max_length=100, verbose_name='nombre')

    class Meta:
        verbose_name = 'provincia'

    def __str__(self):
        return "{}".format(self.name)


class Address(models.Model):
    search_text = models.CharField(max_length=250, null=True, verbose_name='texto de búsqueda')
    name = models.CharField(max_length=250, verbose_name='nombre')
    street = models.CharField(max_length=100, blank=True, null=True, verbose_name='calle')
    house_number = models.IntegerField(blank=True, null=True, verbose_name='altura')
    lat = models.DecimalField(max_digits=1000, decimal_places=10, verbose_name='latitud')
    lon = models.DecimalField(max_digits=1000, decimal_places=10, verbose_name='longitud')
    type = models.ForeignKey(AddressType, verbose_name='tipo')
    district = models.CharField(max_length=100, blank=True, null=True, verbose_name='distrito')
    suburb = models.CharField(max_length=100, blank=True, null=True, verbose_name='suburbio')
    city = models.ForeignKey(City, blank=True, null=True, verbose_name='ciudad')
    state = models.ForeignKey(State, blank=True, null=True, verbose_name='provincia')
    postal_code = models.CharField(max_length=8, blank=True, null=True, verbose_name='código postal')
    source = models.CharField(max_length=6, choices=SOURCES, verbose_name='fuente')

    class Meta:
        verbose_name = 'dirección'
        verbose_name_plural = 'direcciones'

    def __str__(self):
        return '{} {}'.format(self.name, self.house_number)
