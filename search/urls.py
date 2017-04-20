from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^$', views.search, name='search'),
    # url(r'^save_address', views.save_address, name='save_address'),
]
