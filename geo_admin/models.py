from django.db import models


class State(models.Model):
    code = models.CharField(max_length=2, blank=True, null=True, unique=True, verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True, verbose_name='nombre')

    class Meta:
        verbose_name = 'provincia'

    def __str__(self):
        return self.name


class Department(models.Model):
    code = models.CharField(max_length=5, blank=True, null=True, unique=True,  verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True, verbose_name='nombre')
    state = models.ForeignKey(State, blank=True, null=True, verbose_name='provincia')

    class Meta:
        verbose_name = 'departamento'

    def __str__(self):
        return self.name


class Locality(models.Model):
    code = models.CharField(max_length=8, blank=True, null=True, unique=True, verbose_name='código')
    name = models.CharField(max_length=100, blank=True, null=True, verbose_name='nombre')
    agglomerate = models.CharField(max_length=4, blank=True, null=True,verbose_name='aglomerado')
    department = models.ForeignKey(Department, blank=True, null=True, verbose_name='departamento')
    state = models.ForeignKey(State, blank=True, null=True, verbose_name='provincia')

    class Meta:
        verbose_name = 'localidad'
        verbose_name_plural = 'localidades'

    def __str__(self):
        return self.name


class Road(models.Model):
    code = models.CharField(max_length=13, verbose_name='código')
    name = models.CharField(max_length=5, verbose_name='nombre')
    road_type = models.CharField(max_length=25, blank=True, null=True, verbose_name='tipo de camino')
    start_left = models.IntegerField(blank=True, null=True, verbose_name='inicio izquierda')
    start_right = models.IntegerField(blank=True, null=True, verbose_name='inicio derecha')
    end_left = models.IntegerField(blank=True, null=True, verbose_name='fin izquierda')
    end_right = models.IntegerField(blank=True, null=True, verbose_name='fin derecha')
    geom = models.CharField(max_length=100, blank=True, null=True, verbose_name='geometría')
    postal_code = models.CharField(max_length=8, blank=True, null=True, verbose_name='código postal')
    locality = models.ForeignKey(Locality, blank=True, null=True, verbose_name='localidad')
    state = models.ForeignKey(State, blank=True, null=True, verbose_name='provincia')

    class Meta:
        verbose_name = 'calle'

    def __str__(self):
        return self.name
