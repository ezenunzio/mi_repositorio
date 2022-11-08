from django.urls import path
from mvt.views import mostrar_familia


urlpatterns = [
    path("", mostrar_familia),
]