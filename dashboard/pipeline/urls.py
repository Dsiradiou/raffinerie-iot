from django.urls import path
from . import views

urlpatterns = [
    path('',                    views.index,            name='index'),
    path('pipeline/start/',     views.pipeline_start,   name='pipeline_start'),
    path('pipeline/stop/',      views.pipeline_stop,    name='pipeline_stop'),
    path('pipeline/status/',    views.pipeline_status,  name='pipeline_status'),
    path('config/capteurs/',    views.config_capteurs,  name='config_capteurs'),
    path('config/seuils/',      views.config_seuils,    name='config_seuils'),
]