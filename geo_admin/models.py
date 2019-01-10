# -*- coding: utf-8 -*-

from django.contrib.gis.db import models


class Country(models.Model):
    name = models.CharField(max_length=100, verbose_name='nombre')
    iso_code = models.CharField(blank=True, max_length=4,
                                verbose_name='codigo iso')
    iso_name = models.CharField(blank=True, max_length=100,
                                verbose_name='nombre iso')
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')
    geom = models.MultiPolygonField()


class State(models.Model):
    code = models.CharField(max_length=2, unique=True, verbose_name='código')
    name = models.CharField(max_length=100, verbose_name='nombre')
    name_short = models.CharField(max_length=100, verbose_name='nombre corto')
    iso_code = models.CharField(max_length=4, verbose_name='código iso')
    iso_name = models.CharField(max_length=100,
                                verbose_name='nombre iso')
    category = models.CharField(max_length=50, verbose_name='categoría')
    source = models.CharField(max_length=50, verbose_name='fuente')
    # country = models.ForeignKey(Country, verbose_name='país')
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')
    geom = models.MultiPolygonField()

    class Meta:
        verbose_name = 'provincia'

    def __str__(self):
        return self.name


class Department(models.Model):
    code = models.CharField(max_length=5, unique=True, verbose_name='código')
    name = models.CharField(max_length=100, verbose_name='nombre')
    name_short = models.CharField(max_length=100, verbose_name='nombre corto')
    category = models.CharField(max_length=50, verbose_name='categoría')
    source = models.CharField(max_length=50, verbose_name='fuente')
    state = models.ForeignKey(State, verbose_name='provincia')
    state_intersection = models.FloatField(verbose_name="intersección")
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')
    geom = models.MultiPolygonField()

    class Meta:
        verbose_name = 'departamento'

    def __str__(self):
        return ', '.join([self.name, self.state.name])


class Municipality(models.Model):
    code = models.CharField(max_length=6, unique=True, verbose_name='código')
    name = models.CharField(max_length=150, verbose_name='nombre')
    name_short = models.CharField(max_length=100, verbose_name='nombre corto')
    category = models.CharField(blank=True, null=True, max_length=50,
                                verbose_name='categoría')
    source = models.CharField(max_length=50, verbose_name='fuente')
    state = models.ForeignKey(State, verbose_name='provincia')
    state_intersection = models.FloatField(verbose_name="intersección")
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')
    geom = models.MultiPolygonField()

    class Meta:
        verbose_name = 'municipio'


class Settlement(models.Model):
    BAHRA_TYPES = (
        ('E', 'Entidad (E)'),
        ('LC', 'Componente de localidad compuesta (LC)'),
        ('LS', 'Localidad simple (LS)'),
    )
    code = models.CharField(max_length=11, unique=True, verbose_name='código')
    name = models.CharField(max_length=100, verbose_name='nombre')
    category = models.CharField(max_length=3, choices=BAHRA_TYPES,
                                verbose_name='categoría')
    source = models.CharField(max_length=50, verbose_name='fuente')
    municipality = models.ForeignKey(Municipality, blank=True, null=True,
                                     verbose_name='municipalidad')
    department = models.ForeignKey(Department, verbose_name='departamento')
    state = models.ForeignKey(State, verbose_name='provincia')
    lat = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='latitud')
    lon = models.DecimalField(max_digits=9, decimal_places=6,
                              verbose_name='longitud')
    geom = models.MultiPointField()

    class Meta:
        verbose_name = 'entidad bahra'
        verbose_name_plural = 'entidades bahra'

    def __str__(self):
        return ', '.join([self.name, self.state.name])


class Road(models.Model):
    code = models.CharField(max_length=13, verbose_name='código', unique=True)
    name = models.CharField(max_length=100, verbose_name='nombre')
    category = models.CharField(max_length=25, verbose_name='tipo de camino')
    source = models.CharField(max_length=50, verbose_name='fuente')
    start_left = models.IntegerField(blank=True, null=True,
                                     verbose_name='inicio izquierda')
    start_right = models.IntegerField(blank=True, null=True,
                                      verbose_name='inicio derecha')
    end_left = models.IntegerField(blank=True, null=True,
                                   verbose_name='fin izquierda')
    end_right = models.IntegerField(blank=True, null=True,
                                    verbose_name='fin derecha')
    locality_code = models.CharField(max_length=8,
                                     verbose_name='código localidad')
    dept = models.ForeignKey(Department, verbose_name='departamento')
    state = models.ForeignKey(State, verbose_name='provincia')
    geom = models.MultiLineStringField(verbose_name='geometría')

    class Meta:
        verbose_name = 'calle'

    def __str__(self):
        return ', '.join([self.name, self.dept.name, self.state.name])

