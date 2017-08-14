from django.conf.urls import url
from . import views


urlpatterns = [
    url(r'^save_address_search', views.save_address_search, name='save_address_search'),
]
