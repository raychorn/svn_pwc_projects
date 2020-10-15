"""pyextract URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.conf.urls import url

from django.views.generic import TemplateView

from pyextract.views import main_view

from pyextract.views import configs
from pyextract.views import choose_ecf_file
from pyextract.views import preview_ecf_file
from pyextract.views import preview_ecf_file2
from pyextract.views import manage_saved_connections

from pyextract.views import handle_connection
from pyextract.views import get_login_token

from pyextract.views import handle_user_login

from pyextract.rest import read_ecf_file
from pyextract.rest import get_rest_token
from pyextract.rest import save_ecf_connection_file
from pyextract.rest import read_ecf_connection_file


urlpatterns = [
    url('configs', configs),
    url('choose_ecf_file', choose_ecf_file),
    url('preview_ecf_file2', preview_ecf_file2),
    url('preview_ecf_file', preview_ecf_file),
    url('manage_saved_connections', manage_saved_connections),
    url('handle_connection', handle_connection),
    url('handle_user_login', handle_user_login),
    url('get_login_token', get_login_token),
    url('rest/read_ecf_file', read_ecf_file),
    url('rest/get_rest_token', get_rest_token),
    url('rest/save_ecf_connection_file', save_ecf_connection_file),
    url('rest/read_ecf_connection_file', read_ecf_connection_file),
    url('admin/', admin.site.urls),
#    url('', TemplateView.as_view(template_name='main.html')),
    url('', main_view),
]
