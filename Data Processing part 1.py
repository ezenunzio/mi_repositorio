import pandas as pd

# Cargamos el archivo Excel desde una URL en un DataFrame de pandas
# En este caso, es un dataset público de títulos de Netflix
# Nota: Se requiere conexión a Internet para que esta línea funcione correctamente
df = pd.read_excel("https://public.tableau.com/app/sample-data/netflix_titles.xlsx")

# Mostramos las primeras 5 filas del DataFrame
print(df.head())

# Intentamos convertir la columna 'release_year' a tipo entero de 16 bits
# Primero llenamos valores nulos con un año ficticio (ejemplo: 1900)
df["release_year"] = df["release_year"].fillna(1900).astype("int16")


# Inspeccionamos la estructura del DataFrame, incluyendo el uso de memoria
# 'memory_usage="deep"' nos permite ver el uso real de memoria considerando los objetos en memoria
df.info(memory_usage='deep')

# Contamos la cantidad de valores nulos en la columna 'show_id'
nulos_show_id = df["show_id"].isnull().sum()
print(f"Valores nulos en 'show_id': {nulos_show_id}")

# Eliminamos las filas donde 'show_id' es nulo
df = df.dropna(subset=["show_id"])

# Verificamos nuevamente si quedan valores nulos en 'show_id'
nulos_show_id = df["show_id"].isnull().sum()
print(f"Valores nulos en 'show_id' después de eliminar nulos: {nulos_show_id}")

# Inspeccionamos la memoria nuevamente después de eliminar valores nulos
df.info(memory_usage="deep")

# Diccionario para imputar valores en campos con datos faltantes
# En este caso, rellenamos con valores específicos que representen "sin datos"
imputation_mapping = {
    "duration_minutes": -1,  # Si la duración en minutos está vacía, asignamos -1
    "duration_seasons": -1,  # Si la cantidad de temporadas está vacía, asignamos -1
    "date_added": "1900-01-01 00:00:00",  # Fecha mínima ficticia
    "rating": "Not found"  # Valor por defecto si el rating no está disponible
}

# Aplicamos la imputación de valores nulos con el diccionario definido
df = df.fillna(imputation_mapping)

# Mostramos las primeras 10 filas después de la imputación de valores nulos
print(df.head(10))

# Exploramos los valores únicos en la columna 'type'
print("Valores únicos en 'type':", df["type"].unique())

# Exploramos los valores únicos en la columna 'rating'
print("Valores únicos en 'rating':", df["rating"].unique())

# Diccionario para convertir los tipos de datos a tipos más eficientes y apropiados
conversion_mapping = {
    "duration_minutes": "int8",  # Reducimos el uso de memoria usando un entero pequeño
    "duration_seasons": "int8",  # Lo mismo para la cantidad de temporadas
    "date_added": "datetime64[ns]",  # Convertimos a tipo fecha
    "release_year": "int16",  # Entero de 16 bits suficiente para almacenar años
    "type": "category",  # Optimizamos 'type' como categoría (Menos espacio en memoria)
    "rating": "category",  # Lo mismo para 'rating'
    "title": "string"  # Convertimos los títulos a formato de cadena optimizado
}

# Aplicamos la conversión de tipos al DataFrame
df = df.astype(conversion_mapping)

# Inspeccionamos la memoria nuevamente después de la conversión de tipos
df.info(memory_usage='deep')

# Agrupamos los datos por 'type' y 'rating' y calculamos estadísticas agregadas
agg_df = df.groupby(['type', 'rating']).agg(
    {
        'duration_minutes': 'mean',  # Media de la duración en minutos
        'duration_seasons': 'mean',  # Media de la duración en temporadas
        'show_id': 'count'  # Cantidad de títulos disponibles en cada grupo
    }
).rename(
    columns={
        'duration_minutes': 'mean_duration_minutes',
        'duration_seasons': 'mean_duration_seasons',
        'show_id': 'count_show_id'
    }
)

print(agg_df)

# Creamos una tabla dinámica (pivot table) para visualizar los datos de manera diferente
pivot_df = pd.pivot_table(
    df,
    index='type',  # Indexamos por 'type' (Películas o series)
    columns='rating',  # Columnas por rating
    values=['duration_minutes', 'show_id'],  # Analizamos duración y cantidad de títulos
    aggfunc=['mean', 'count']  # Calculamos la media y la cantidad de títulos por rating
)

print(pivot_df)