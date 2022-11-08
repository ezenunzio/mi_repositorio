from django.shortcuts import render
from mvt.models import familia

def mostrar_familia(request):
    
   f1 = familia(nombre="Adriana", apellido="Alonso", edad="52", cumpleanios= "1970-05-15")
   f2 = familia(nombre="Mariana", apellido="Padilla", edad="24", cumpleanios= "1998-09-05")
   f3 = familia(nombre="Gabriel", apellido="Padilla", edad="20", cumpleanios= "2002-06-24")
   f4 = familia(nombre="Javier", apellido="Padilla", edad="49", cumpleanios= "1973-07-07")
   return render(request, 'mvt/familia.html', {'familia':[f1, f2, f3, f4]})