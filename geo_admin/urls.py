from django.conf.urls import url
from django.contrib.auth import views as auth_views


urlpatterns = [
    url(r'^login/$',
        auth_views.login, {'template_name': 'login.html'}, name='login'),
]
