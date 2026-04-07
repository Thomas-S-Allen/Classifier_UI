from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("api/connect", views.api_connect, name="api_connect"),
    path("api/query", views.api_query, name="api_query"),
    path("api/record", views.api_record, name="api_record"),
    path("api/update", views.api_update, name="api_update"),
]
