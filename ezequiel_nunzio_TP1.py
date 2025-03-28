import requests
import pandas as pd
from deltalake import write_deltalake, DeltaTable


# Parámetros de la API para 
BASE_URL = "https://data.police.uk/api"
ENDPOINT_CRIMES_STREET = "crimes-street/all-crime"
ENDPOINT_CATEGORIES = "crime-categories"
MONTHS = ["2024-01", "2024-02", "2024-03"]
AREA_POLY = "52.268,0.543:52.794,0.238:52.130,0.478"

BRONZE_PATH = "data/bronze/crimes"
SILVER_PATH = "data/silver/crimes"
GOLD_PATH = "data/gold/crimes"

def get_data(base_url, endpoint, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos en formato JSON.
    """
    try:
        url = f"{base_url}/{endpoint}"
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud: {e}")
        return None
    except ValueError:
        print("Error al procesar la respuesta JSON")
        return None

def fetch_crime_categories(base_url, endpoint, months):
    """
    Obtiene las categorías de crimen para múltiples meses.
    """
    all_categories = []
    for month in months:
        params = {"date": month}
        data = get_data(base_url, endpoint, params=params)
        if data:
            all_categories.extend(data)
    
    return pd.DataFrame(all_categories) if all_categories else pd.DataFrame()

def fetch_crime_data(base_url, endpoint, months, area_poly):
    """
    Obtiene datos de crímenes para múltiples meses y devuelve un DataFrame.
    """
    all_crimes = []
    for month in months:
        params = {"date": month, "poly": area_poly}
        data = get_data(base_url, endpoint, params=params)
        if data:
            all_crimes.extend(data)
    return pd.DataFrame(all_crimes) if all_crimes else pd.DataFrame()


raw__crimes_street = fetch_crime_data(BASE_URL, ENDPOINT_CRIMES_STREET, MONTHS, AREA_POLY)
raw__crime_categories = fetch_crime_categories(BASE_URL, ENDPOINT_CATEGORIES, MONTHS)

write_deltalake(BRONZE_PATH, raw__crimes_street)


# 🔹 Procesar datos para la capa Silver
def process_crime_data(df):
    """
    Limpia y transforma los datos crudos para almacenarlos en la capa Silver del Lakehouse.
    
    Parámetros:
    - df (DataFrame): Datos sin procesar obtenidos de la capa Bronze.

    Retorna:
    - DataFrame limpio y optimizado para la capa Silver.
    """
    
    dt = DeltaTable(BRONZE_PATH)
    df = dt.to_pandas()  # Convertir a Pandas para manipular


    # Eliminar registros sin ID único (inconsistentes)
    df = df.dropna(subset=["persistent_id"])
    

    # Extraer datos de 'location' y 'outcome_status' de forma eficiente
    df["latitude"] = df["location"].apply(lambda x: x.get("latitude") if isinstance(x, dict) else None)
    df["longitude"] = df["location"].apply(lambda x: x.get("longitude") if isinstance(x, dict) else None)
    df["street_name"] = df["location"].apply(lambda x: x.get("street", {}).get("name") if isinstance(x, dict) else None)
    df["outcome_category"] = df["outcome_status"].apply(lambda x: x.get("category", "") if isinstance(x, dict) else "")
    df["outcome_date"] = df["outcome_status"].apply(lambda x: x.get("date", "") if isinstance(x, dict) else "")
    
    # Hacer el merge con la tabla de categorías de crimen
    df = df.merge(
    raw__crime_categories, 
    left_on="category", 
    right_on="url", 
    how="left"
    )

    # Renombrar columnas
    df = df.rename(columns={
        "persistent_id": "crime_persistent_id",
        "month": "crime_month",
        "location_type": "location_type",
        "context": "crime_context",
        "name": "crime_category"
    })
    
    # Eliminar columnas innecesarias
    df = df.drop(columns=["location", "id", "outcome_status", "category", "url"])

    # Convertir tipos de datos
    df["crime_month"] = pd.to_datetime(df["crime_month"], format="%Y-%m", errors="coerce")
    # Asegurar que los valores NaN se llenen antes de la conversión
    df["outcome_date"] = df["outcome_date"].astype(str).replace("nan", None)
    # Convertir a datetime y reemplazar NaN con una fecha específica
    df["outcome_date"] = pd.to_datetime(df["outcome_date"], errors="coerce").fillna(pd.Timestamp("1899-12-31"))
    # Crear columna con el formato de fecha "M  es-Año" para el outcome_date
    df["outcome_month_year"] = df["outcome_date"].dt.strftime("%b-%y").fillna("")
    # Limpieza de nombres de calles
    df["street_name"] = df["street_name"].str.replace(r"^On or near ", "", regex=True)

    # Convertir a tipos eficientes
    conversion_mapping = {
        "crime_category": "category",
        "crime_persistent_id": "string",
        "latitude": "float32",
        "longitude": "float32",
        "street_name": "category",
        "location_type": "category",
        "crime_context": "string",
        "outcome_category": "category"
    }
    
    # Aplicar conversión solo a las columnas existentes
    for col, dtype in conversion_mapping.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    return df

processed_crimes_df = process_crime_data(raw__crimes_street)
write_deltalake(SILVER_PATH, processed_crimes_df, mode="overwrite")

processed_crimes_df








