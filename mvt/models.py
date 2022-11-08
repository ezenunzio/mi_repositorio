from django.db import models

# Create your models here.
class familia(models.Model):

    nombre = models.TextField()
    apellido = models.TextField()
    edad = models.IntegerField()
    cumpleanios = models.DateField()