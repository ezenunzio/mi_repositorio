from django.shortcuts import render
from .models import Familia
# Create your views here.



def mostrar_familia(request):
    
   f1 = Familia(nombre="Pablo", apellido="Nunzio", edad=55, cumpleanios='1967-04-11')
   f2 = Familia(nombre="Lisandro", apellido="Nunzio", edad=25, cumpleanios='1997-03-09')
   f3 = Familia(nombre="Ezequiel", apellido="Nunzio", edad=22,cumpleanios='2000-09-10')
   f4 = Familia(nombre="Carina", apellido="Martinez", edad=51, cumpleanios='1963-08-11')
   return render(request, 'familiares.html', {'familia':[f1, f2, f3, f4]})



def mostrar_index(request):
    return render(request, 'index.html')

    
def mostrar_referencias(request):
    return render(request, 'referencias.html')

def mostrar_repaso(request):
    return render(request, 'repaso.html')