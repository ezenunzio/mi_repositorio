from django.http import HttpResponse
from django.shortcuts import render


def saludo(request):
    return HttpResponse("Hola Django -Coder")


def mostrar_html(request):
    return render(request, 'template1.html', {'nombre' : 'Ezequiel', 'lista': [1,2,3,4]})




