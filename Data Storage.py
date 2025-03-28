import pandas as pd
import pyarrow as pa
import os

# Intentamos importar las funciones de la librería Delta Lake.
# Si no está instalada, mostramos un mensaje de error y detenemos la ejecución.
try:
    from deltalake import write_deltalake, DeltaTable
except ModuleNotFoundError:
    print("❌ Error: El módulo 'deltalake' no está instalado. Instálalo con 'pip install deltalake'.")
    exit(1)

# 📌 Definimos dos conjuntos de datos simulados en forma de diccionarios
# Representan clientes con su ID, nombre y correo electrónico.

data_1 = {
    "client_id": [1, 2, 3],
    "client_name": ["Mario Santos", "Emilio Ravenna", "Gabriel Medina"],
    "client_email": ["mario@gmail.com", "emilio@gmail.com", "gabriel@gmail.com"],
}

data_2 = {
    "client_id": [1, 4, 5],
    "client_name": ["Mario Santos", "Franco Milazzo", "Marcos Molero"],
    "client_email": ["mariosantos@gmail.com", "francomilazzo@gmail.com", "marcosmolero@gmail.com"]
}

# 📌 Convertimos los diccionarios en DataFrames de Pandas
df_1 = pd.DataFrame(data_1)
df_2 = pd.DataFrame(data_2)

# 📌 Escritura en formato Delta Lake con el modo "error"
# Si la ruta ya contiene datos, se generará un error.
write_deltalake(
    "data/clients",  # Ruta donde se almacenarán los datos
    df_1,  # DataFrame a escribir
    mode="error"  # Si ya existe, genera error
)

# Intentamos escribir un segundo DataFrame en la misma ubicación con modo "error".
# Esto fallará porque "data/clients" ya tiene datos.
write_deltalake(
    "data/clients",
    df_2,
    mode="error"
)

# 📌 Escritura en formato Delta Lake con el modo "ignore"
# En este modo, si la tabla ya existe, no se sobrescribirá ni generará error.
write_deltalake(
    "data/clients_ignore",
    df_1,
    mode="ignore"
)

# Intentamos escribir nuevamente con modo "ignore".
# Como la tabla ya existe, no se realizará ninguna modificación.
write_deltalake(
    "data/clients_ignore",
    df_2,
    mode="ignore"
)

# 📌 Escritura con el modo "overwrite"
# En este modo, cualquier dato previo en "data/clients_overwrite" será eliminado y reemplazado.
write_deltalake(
    "data/clients_overwrite",
    df_1,
    mode="overwrite"
)

# Escribimos nuevamente con modo "overwrite", por lo que df_2 reemplazará completamente df_1.
write_deltalake(
    "data/clients_overwrite",
    df_2,
    mode="overwrite"
)

# 📌 Listamos los archivos en la carpeta "data/clients_overwrite" para verificar los cambios.
os.listdir("data/clients_overwrite")

os.listdir("data/clients_overwrite/_delta_log")

write_deltalake(
    "data/clients_append_2",
    df_1,
    mode="error"
    )

write_deltalake(
    "data/clients_append_2",
    df_2,
    mode="append"
    )

write_deltalake(
    "data/clients_merge",
    df_1
    )

# Los datos actuales ya estan en Delta lake, debemos leerlos
actual_data = DeltaTable("data/clients_merge")
print(actual_data.to_pandas())
print(df_2)


# Creamos una tabla pyarrow, representando datos nuevos que estan en un dataframe
new_data = pa.Table.from_pandas(df_2)

"""
A continuación se ve la aplicación de la operación MERGE, donde se actualizan los datos de la columna `client_mail`.

Si entre `actual_data` y `new_data` hay registros con el mismo `client_id`, se actualizará el `client_mail` con el valor que haya en `new_data`.

Si no hay registros con el mismo `client_id`, se insertará el registro de `new_data`.
"""


(
    actual_data.merge(
        source=new_data,  # los datos nuevos a insertar o actualizar
        source_alias="src",  # src -> new_data
        target_alias="tgt",  # tgt -> actual_data
        predicate="src.client_id = tgt.client_id"  # Condición para hacer la comparación
    )
    .when_matched_update(
        updates={
            "client_email": "src.client_email"  # Actualiza el mail del cliente de new_data
            # Puedes agregar más campos a actualizar si lo deseas, por ejemplo:
            # "client_name": "src.client_name"  # Si quieres actualizar también el nombre
        }
    )
    .when_not_matched_insert_all()  # Inserta todos los datos de new_data que no existan en actual_data
    .execute()  # Ejecuta la operación de merge
)

print(actual_data.to_pandas().sort_values("client_id"))

DeltaTable("data/clients_merge").to_pandas().sort_values("client_id")

# Vamos a leer alguna de las tablas Delta ya creadas
# y agregamos la restricción de que el campo client_id debe ser mayor a 0
dt = DeltaTable("data/clients_merge")
dt.alter.add_constraint(
    # alias de la restriccion : restriccion/regla
    {"client_id_gt_0": "client_id > 0"}
)

# Creamos un DataFrame con datos no válidos
data_no_valid = {
    "client_id": [9, 0],
    "client_name": ["anonimo", "Pablo Lampone"],
    "client_email": ["anonimo", "lampone@gmail.com"]
}
df_no_valid = pd.DataFrame(data_no_valid)
df_no_valid


# Intentamos escribir los datos no válidos en la tabla Delta
write_deltalake(
    "data/clients_merge",
    df_no_valid,
    mode="append",
    engine="rust"
    )

write_deltalake(
    "data/clients_partitioned",
    df_1,
    mode="append",
    partition_by="client_id"
)

os.listdir("data/clients_partitioned")

dt = DeltaTable("data/clients_merge").optimize
