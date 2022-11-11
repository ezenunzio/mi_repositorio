from django.shortcuts import render
from .models import familia
# Create your views here.



def mostrar_familia(request):
    
   f1 = familia(nombre="Pablo", apellido="Nunzio", edad=55, cumpleanios="04/11/1967")
   f2 = familia(nombre="Lisandro", apellido="Nunzio", edad=25, cumpleanios="1997")
   f3 = familia(nombre="Ezequiel", apellido="Nunzio", edad=22,cumpleanios="26/09/1988")
   f4 = familia(nombre="Carina", apellido="Martinez", edad=51, cumpleanios="11/08/1963")
   return render(request, 'template1.html', {'familia':[f1, f2, f3, f4]})