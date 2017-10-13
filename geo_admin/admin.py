from django.contrib import admin
from geo_admin.models import Consumer, Department, Locality, Road, State


admin.site.register(Consumer)
admin.site.register(Department)
admin.site.register(Locality)
admin.site.register(Road)
admin.site.register(State)
