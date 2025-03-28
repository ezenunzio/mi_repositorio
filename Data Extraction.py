import requests
from pprint import pprint


url = "https://api.github.com"
r = requests.get(url)
data = r.json()
campo = "code_search_url"

#print(r)
#print(type(r))
#print(f"El codigo de respuesta es : {r.status_code}")
#print(data[campo])
#print(dir(r))
#print(data.keys())


r = requests.get(
    url='https://api.github.com/search/repositories',
    params={ #Parametros se pasan como un Diccionario
        "q": "java",
        }
    )

if r.status_code == 200:
  print(f"La peticion fue exitosa, se obutvo una respuesta de tipo: {type(r.json())}")
else:
  print(f"Error en la petición: {r.status_code, r.content}")
  
pprint(r.json())

import requests  # Para hacer solicitudes HTTP
import pandas as pd  # Para manejar datos en estructura de DataFrame
from datetime import datetime, timedelta  # Para manipulación de fechas
from pprint import pprint  # Para imprimir JSON de manera legible

# 🔹 Función para obtener datos desde una API
def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos en formato JSON.

    Parámetros:
    - base_url (str): URL base de la API.
    - endpoint (str): Endpoint específico dentro de la API.
    - data_field (str, opcional): Clave del JSON que contiene los datos deseados.
    - params (dict, opcional): Parámetros de consulta (ejemplo: filtros o paginación).
    - headers (dict, opcional): Encabezados HTTP (ejemplo: autenticación con tokens).

    Retorna:
    - dict o list: Datos obtenidos de la API o None si hay un error.
    """
    try:
        # Construcción de la URL completa
        url = f"{base_url}/{endpoint}"
        
        # Realiza la solicitud GET a la API
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Lanza un error si hay problemas en la respuesta HTTP
        
        # Intenta convertir la respuesta a JSON
        data = response.json()
        
        # Si se especificó un campo, devuelve solo esa parte del JSON; si no, todo el JSON
        return data.get(data_field, data) if data_field else data

    except requests.exceptions.RequestException as e:
        print(f"❌ Error en la solicitud: {e}")
        return None
    except ValueError:
        print("❌ Error al procesar la respuesta JSON")
        return None


# 🔹 Función para convertir JSON en DataFrame de pandas
def build_table(json_data):
    """
    Convierte datos JSON en un DataFrame de pandas.

    Parámetros:
    - json_data (dict o list): Datos en formato JSON obtenidos de la API.

    Retorna:
    - pd.DataFrame: DataFrame con los datos o None si hay error.
    """
    if not json_data:  # Verifica si los datos son válidos
        print("⚠️ No se proporcionaron datos válidos")
        return None

    try:
        return pd.json_normalize(json_data)  # Convierte JSON en DataFrame
    except Exception as e:
        print(f"❌ Error al convertir JSON en DataFrame: {e}")
        return None


# 🔹 Configuración de la API
base_url = "https://api.luchtmeetnet.nl/open_api"

# 1️⃣ Obtener lista de estaciones
endpoint = "stations"
json_data = get_data(base_url, endpoint, data_field="data")

# Imprimir JSON de estaciones
# pprint(json_data)

# Verificar tipo de datos obtenidos
print(f"Tipo de json_data: {type(json_data)}, Tipo de un elemento: {type(json_data[0])}")

# Obtener componentes medidos en las estaciones
endpoint = "components"
json_data = get_data(base_url, endpoint, data_field="data")
df_components = build_table(json_data)

# Mostrar primeras filas de componentes
print(df_components.head())


# 2️⃣ Obtener todas las estaciones con un parámetro de filtro
params = {"organisation_id": "1"}
stations = get_data(base_url, "stations", "data", params=params)

if stations:
    df_stations = build_table(stations)
    print(df_stations.head())  # Muestra primeras filas de las estaciones

# 3️⃣ Obtener detalles de cada estación
all_stations = []

for station in stations:
    endpoint = f"stations/{station['number']}"
    station_details = get_data(base_url, endpoint, data_field="data")

    if station_details:
        station_details["number"] = station["number"]  # Agregar número de estación
        all_stations.append(station_details)

df_station_details = build_table(all_stations)
print(df_station_details.head())


# 4️⃣ Obtener mediciones de la última hora de diferentes estaciones
current_time = datetime.utcnow()  # Hora actual en UTC
start_time = current_time - timedelta(hours=2)  # Hace 2 horas

# Formatear fechas en formato ISO 8601 para la API
params = {
    "start": start_time.strftime("%Y-%m-%dT%H:00:00Z"),
    "end": start_time.strftime("%Y-%m-%dT%H:59:59Z")
}

# Obtener mediciones
measurements = get_data(base_url, "measurements", "data", params=params)
df_measurements = build_table(measurements)

print(df_measurements.head())  # Muestra las primeras filas de mediciones
