from django.db import models

# Create your models here.
class Familia(models.Model):

    nombre = models.TextField(max_length=40)
    apellido = models.TextField(max_length=40)
    edad = models.IntegerField()
    cumpleanios = models.DateField()




    


    

