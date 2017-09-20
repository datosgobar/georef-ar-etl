from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^$', views.search, name='search'),
    url(r'^normalizar', views.normalize, name='normalize'),
    url(r'^comparar', views.compare, name='compare'),
    url(r'^save_address', views.save_address, name='save_address'),
    url(r'^token', views.generate_token, name='generate_token'),
]
