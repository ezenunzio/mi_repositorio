import requests
import pandas as pd
from deltalake import write_deltalake, DeltaTable

# Parámetros de la API para obtener datos sobre crímenes y categorías
BASE_URL = "https://data.police.uk/api"
ENDPOINT_CRIMES_STREET = "crimes-street/all-crime"  # Endpoint para obtener crímenes por calle
ENDPOINT_CATEGORIES = "crime-categories"  # Endpoint para obtener categorías de crimen
MONTHS = ["2024-01", "2024-02", "2024-03"]  # Lista de meses para los que vamos a extraer datos
AREA_POLY = "52.268,0.543:52.794,0.238:52.130,0.478"  # Área geográfica para los datos de crímenes

# Rutas donde se almacenarán las tablas Delta para diferentes etapas del proceso
BRONZE_PATH = "data/bronze/crimes"  # Capa Bronze: Datos crudos sin procesar
SILVER_PATH = "data/silver/crimes"  # Capa Silver: Datos procesados y limpios

# Función para obtener datos de la API
def get_data(endpoint, params=None):
    """Realiza una solicitud GET a la API y devuelve los datos en formato JSON."""
    try:
        # Realiza una solicitud GET al endpoint de la API
        response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()  # Lanza un error si la respuesta no es exitosa
        return response.json()  # Devuelve los datos en formato JSON
    except requests.RequestException as e:
        print(f"Error en la solicitud: {e}")  # Si ocurre un error en la solicitud, lo muestra
        return []  # Devuelve una lista vacía en caso de error

# Función para obtener los datos de crímenes por calle
def fetch_crime_data():
    """Obtiene datos de crímenes para múltiples meses."""
    # Combina los datos de crímenes para cada mes en un solo DataFrame
    return pd.DataFrame(
        [crime for month in MONTHS for crime in get_data(ENDPOINT_CRIMES_STREET, {"date": month, "poly": AREA_POLY})]
    )

# Función para obtener las categorías de crímenes
def fetch_crime_categories():
    """Obtiene las categorías de crimen."""
    # Llama a la API para obtener las categorías de crimen y las devuelve como un DataFrame
    return pd.DataFrame(get_data(ENDPOINT_CATEGORIES, {"date": MONTHS[0]}))

# Cargar datos crudos de crímenes y categorías en Delta Bronze
raw_crimes_street = fetch_crime_data()
raw_crime_categories = fetch_crime_categories()

# Si se obtienen datos de crímenes, los almacenamos en la tabla Delta de la capa Bronze
if not raw_crimes_street.empty:
    write_deltalake(BRONZE_PATH, raw_crimes_street)

# Función para procesar y limpiar los datos de crímenes
def process_crime_data():
    """Limpia y transforma los datos crudos para la capa Silver del Lakehouse."""
    
    # Cargar datos desde la tabla Delta en la capa Bronze
    dt = DeltaTable(BRONZE_PATH)
    df = dt.to_pandas()  # Convertimos la tabla Delta a un DataFrame de Pandas

    # Verificamos si el DataFrame está vacío y si es así, devolvemos un DataFrame vacío
    if df.empty:
        print("No hay datos para procesar.")
        return pd.DataFrame()

    # Eliminar filas con valores nulos en la columna 'persistent_id' (ID único de crimen)
    df.dropna(subset=["persistent_id"], inplace=True)

    # Extraer datos anidados de las columnas 'location' y 'outcome_status' utilizando 'apply' para vectorizar
    df.loc[:, "latitude"] = df["location"].apply(lambda x: x.get("latitude") if isinstance(x, dict) else None)
    df.loc[:, "longitude"] = df["location"].apply(lambda x: x.get("longitude") if isinstance(x, dict) else None)
    df.loc[:, "street_name"] = df["location"].apply(lambda x: x.get("street", {}).get("name", None) if isinstance(x, dict) else None)
    df.loc[:, "outcome_category"] = df["outcome_status"].apply(lambda x: x.get("category", "") if isinstance(x, dict) else "")
    df.loc[:, "outcome_date"] = pd.to_datetime(
        df["outcome_status"].apply(lambda x: x.get("date", None) if isinstance(x, dict) else None), errors="coerce"
    ).fillna(pd.Timestamp("1899-12-31"))  # Rellenar valores nulos con una fecha de reemplazo

    # Realizar un merge con la tabla de categorías de crimen para añadir información adicional
    df = df.merge(raw_crime_categories, left_on="category", right_on="url", how="left")

    # Renombrar las columnas para hacerlas más comprensibles
    df.rename(columns={
        "persistent_id": "crime_persistent_id",  # Renombrar la columna 'persistent_id' a 'crime_persistent_id'
        "context": "crime_context",  # Renombrar 'context' a 'crime_context'
        "name": "crime_category"  # Renombrar 'name' a 'crime_category'
    }, inplace=True)

    # Eliminar columnas innecesarias que no son útiles para el análisis posterior
    df.drop(columns=["location", "id", "outcome_status", "category", "url"], inplace=True)

    # Crear una nueva columna con el mes y año del resultado de la investigación
    df.loc[:, "outcome_month_year"] = df["outcome_date"].dt.strftime("%b-%y")

    # Limpiar los nombres de calles eliminando la frase "On or near" que puede ser innecesaria
    df.loc[:, "street_name"] = df["street_name"].str.replace(r"^On or near ", "", regex=True)

    # Convertir las columnas a tipos de datos más eficientes para reducir el uso de memoria
    dtype_map = {
        "crime_category": "category",  # Convertir las categorías a 'category' para eficiencia
        "crime_persistent_id": "string",  # Asegurarnos de que los ID sean de tipo string
        "latitude": "float32",  # Reducir el tamaño de memoria de latitudes y longitudes
        "longitude": "float32",  # Similar para longitud
        "street_name": "category",  # Convertir los nombres de calles a 'category'
        "crime_context": "string",  # El contexto de crimen se mantiene como string
        "outcome_category": "category"  # La categoría del resultado también es una categoría
    }

    # Aplicar la conversión de tipos solo a las columnas que existen en el DataFrame
    df = df.astype({col: dtype for col, dtype in dtype_map.items() if col in df.columns})

    return df  # Retornar el DataFrame procesado

# Procesar los datos crudos y almacenarlos en la tabla Delta de la capa Silver
processed_crimes_df = process_crime_data()

# Si el DataFrame procesado no está vacío, lo escribimos en la capa Silver de Delta
if not processed_crimes_df.empty:
    write_deltalake(SILVER_PATH, processed_crimes_df, mode="overwrite")
