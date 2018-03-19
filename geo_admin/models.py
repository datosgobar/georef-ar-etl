from django.contrib.gis.db import models
from django.contrib.auth.models import User


class State(models.Model):
    code = models.CharField(max_length=2, blank=True, null=True, unique=True,
                            verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True,
                            verbose_name='nombre')
    geom = models.PolygonField(blank=True, null=True)
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')

    class Meta:
        verbose_name = 'provincia'

    def __str__(self):
        return self.name


class Department(models.Model):
    code = models.CharField(max_length=5, blank=True, null=True, unique=True,
                            verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True,
                            verbose_name='nombre')
    state = models.ForeignKey(State, blank=True, null=True,
                              verbose_name='provincia')
    geom = models.PolygonField(blank=True, null=True)
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')

    class Meta:
        verbose_name = 'departamento'

    def __str__(self):
        return ', '.join([self.name, self.state.name])


class Municipality(models.Model):
    code = models.CharField(max_length=6, blank=True, null=True, unique=True,
                            verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True,
                            verbose_name='nombre')
    department = models.ForeignKey(Department, blank=True, null=True,
                                   verbose_name='departamento')
    state = models.ForeignKey(State, blank=True, null=True,
                              verbose_name='provincia')
    geom = models.PolygonField(blank=True, null=True)
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')

    class Meta:
        verbose_name = 'municipio'


class Locality(models.Model):
    code = models.CharField(max_length=8, blank=True, null=True, unique=True,
                            verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True,
                            verbose_name='nombre')
    agglomerate = models.CharField(max_length=4, blank=True, null=True,
                                   verbose_name='aglomerado')
    department = models.ForeignKey(Department, blank=True, null=True,
                                   verbose_name='departamento')
    state = models.ForeignKey(State, blank=True, null=True,
                              verbose_name='provincia')

    class Meta:
        verbose_name = 'localidad'
        verbose_name_plural = 'localidades'

    def __str__(self):
        return ', '.join([self.name, self.state.name])


class Settlement(models.Model):
    BAHRA_TYPES = (
        ('E', 'Entidad (E)'),
        ('LC', 'Componente de localidad compuesta (LC)'),
        ('LS', 'Localidad simple (LS)'),
    )
    code = models.CharField(max_length=11, blank=True, null=True, unique=True,
                            verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True,
                            verbose_name='nombre')
    bahra_type = models.CharField(max_length=3, blank=True, null=True,
                                  verbose_name='tipo', choices=BAHRA_TYPES)
    municipality = models.ForeignKey(Municipality, blank=True, null=True,
                                     verbose_name='municipalidad')
    department = models.ForeignKey(Department, blank=True, null=True,
                                   verbose_name='departamento')
    state = models.ForeignKey(State, blank=True, null=True,
                              verbose_name='provincia')
    geom = models.MultiPointField(blank=True, null=True)
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')

    class Meta:
        verbose_name = 'entidad bahra'
        verbose_name_plural = 'entidades bahra'

    def __str__(self):
        return ', '.join([self.name, self.state.name])


class Road(models.Model):
    code = models.CharField(max_length=13, verbose_name='código')
    name = models.CharField(max_length=100, verbose_name='nombre')
    road_type = models.CharField(max_length=25, blank=True, null=True,
                                 verbose_name='tipo de camino')
    start_left = models.IntegerField(blank=True, null=True,
                                     verbose_name='inicio izquierda')
    start_right = models.IntegerField(blank=True, null=True,
                                      verbose_name='inicio derecha')
    end_left = models.IntegerField(blank=True, null=True,
                                   verbose_name='fin izquierda')
    end_right = models.IntegerField(blank=True, null=True,
                                    verbose_name='fin derecha')
    geom = models.TextField(blank=True, null=True, verbose_name='geometría')
    postal_code = models.CharField(max_length=8, blank=True, null=True,
                                   verbose_name='código postal')
    locality = models.ForeignKey(Locality, blank=True, null=True,
                                 verbose_name='localidad')
    state = models.ForeignKey(State, blank=True, null=True,
                              verbose_name='provincia')

    class Meta:
        verbose_name = 'calle'

    def __str__(self):
        return ', '.join([self.name, self.locality.name, self.state.name])


class Consumer(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    consumer_id = models.CharField(max_length=100)
    api_key = models.CharField(max_length=100)
    api_secret = models.CharField(max_length=100)

    class Meta:
        verbose_name = 'consumidor'
        verbose_name_plural = 'consumidores'

    def __str__(self):
        return self.user.username
