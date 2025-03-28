import pandas as pd

# Creamos un DataFrame con información sobre diferentes marcas de cerveza
df = pd.DataFrame(
    {
        "Brand": [
            "Heineken", "Corona", "Budweiser", "Stella Artois", "Guinness",
            "Heineken", "Corona", "Budweiser", "Stella Artois", "Guinness",
        ],
        "Type": [
            "Pilsner", "Pale Lager", "Pale Lager", "Pilsner", "Stout",
            "Pilsner", "Pale Lager", "Pale Lager", "Pilsner", "Stout",
        ],
        "ABV": [5.0, 4.5, 5.0, 5.2, 4.2, 5.0, 4.5, 5.0, 5.2, 4.2],
        "Rating": [4.1, 3.9, 4.2, 4.3, 4.5, 4.1, 3.9, 4.2, 4.3, 4.5],
    }
)

# Ordenamos el DataFrame por 'Brand' y 'Type' para visualizar mejor los duplicados
df = df.sort_values(by=["Brand", "Type"])
print(df)

# Imprimimos la cantidad de filas y columnas antes de eliminar duplicados
print(f"\nFilas: {df.shape[0]}. Columnas: {df.shape[1]}")

# Eliminamos duplicados considerando todas las columnas
df_deduplicated = df.drop_duplicates().copy()
print(f"La cantidad de filas después de eliminar duplicados es {df_deduplicated.shape[0]}")

# Agregamos una columna ID para identificar registros únicos manualmente
df["ID"] = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]

# Eliminamos duplicados manteniendo solo la primera ocurrencia de cada ID
df_deduplicated = df.drop_duplicates(subset="ID", keep="first")

# Eliminamos duplicados basándonos en 'Brand' y 'Type', conservando la primera aparición
df_deduplicated = df.drop_duplicates(subset=["Brand", "Type"], keep="first")

# Eliminamos duplicados basándonos en 'Brand' y 'Type', eliminando todas las ocurrencias duplicadas
df_deduplicated = df.drop_duplicates(subset=["Brand", "Type"], keep=False)

# Creamos un nuevo DataFrame con registros adicionales y una columna de fechas
df = pd.DataFrame({
    'Brand': ['Heineken', 'Corona', 'Budweiser', 'Stella Artois', 'Guinness'] * 3,
    'Type': ['Pilsner', 'Pale Lager', 'Pale Lager', 'Pilsner', 'Stout'] * 3,
    'ID': [1, 2, 3, 4, 5] * 3,
    'Price': [4.1, 3.9, 4.2, 4.3, 4.5, 4.2, 4.1, 4.3, 4.4, 4.6, 4.3, 4.2, 4.4, 4.5, 4.7],
    'Date_price': [
        '2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04', '2021-01-05',
        '2021-02-01', '2021-02-02', '2021-02-03', '2021-02-04', '2021-02-05',
        '2021-03-01', '2021-03-02', '2021-03-03', '2021-03-04', '2021-03-05'
    ]
})

# Ordenamos por 'ID' y 'Date_price' para gestionar duplicados con base en la fecha más reciente
df = df.sort_values(by=["ID", "Date_price"], ascending=[False, True])

# Eliminamos duplicados manteniendo el registro más reciente por 'ID'
df_deduplicated = df.drop_duplicates(subset=["ID"], keep="last")

# Alternativamente, ordenamos en orden descendente por fecha antes de eliminar duplicados
df_deduplicated = (
    df
    .sort_values(by=["ID", "Date_price"], ascending=[False, False])
    .drop_duplicates(subset=["ID"], keep="first")
)
