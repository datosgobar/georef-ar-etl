from django.contrib import admin
from geo_admin.models import Department, Locality, Road, State


admin.site.register(Department)
admin.site.register(Locality)
admin.site.register(Road)
admin.site.register(State)
