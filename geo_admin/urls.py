from django.conf.urls import url
from django.contrib.auth import views as auth_views
from . import views


urlpatterns = [
    url(r'^login/$',
        auth_views.login, {'template_name': 'login.html'}, name='login'),
    url(r'^token', views.get_token, name='get_token'),
    url(r'^metricas', views.get_api_metrics, name='api_metrics'),
]
