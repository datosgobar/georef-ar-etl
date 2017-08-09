from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^$', views.search, name='search'),
    url(r'^normalizar', views.normalize, name='normalize'),
    url(r'^comparar', views.compare, name='compare'),
    url(r'^save_address_search', views.save_address_search, name='save_address_search'),
    url(r'^save_address', views.save_address, name='save_address'),
]
