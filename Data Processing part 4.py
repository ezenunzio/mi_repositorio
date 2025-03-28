pip install faker

import pandas as pd
import faker

# Función para generar un DataFrame ficticio con logs de servidor
def generate_fake_logs(n):
    fake = faker.Faker()
    logs = []
    for _ in range(n):
        ip = fake.ipv4()  # Generamos una dirección IP
        tstamp = fake.date_time_this_year()  # Generamos un timestamp dentro del año actual
        method = fake.random_element(["GET", "POST", "PUT", "DELETE"])  # Seleccionamos un método HTTP aleatorio
        url = fake.uri()  # Generamos una URL aleatoria
        request = f"{method} {url}"  # Construimos la petición HTTP
        status = fake.random_int(100, 599)  # Generamos un código de estado HTTP aleatorio
        bytes = fake.random_int(100, 10000)  # Simulamos la cantidad de bytes transmitidos
        user_agent = fake.user_agent()  # Generamos un user agent aleatorio
        logs.append(f'{ip} - {tstamp} - "{request}" - {status} - {bytes} - "{user_agent}"')

    return pd.DataFrame(logs, columns=["log"])

# Generamos 1000 logs ficticios
df = generate_fake_logs(1000)

# Configuramos pandas para visualizar datos sin truncar
pd.set_option('display.max_colwidth', None)
print(df.head())

# Separar la columna 'log' en múltiples columnas estructuradas
df_structured = df["log"].str.split(" - ", expand=True, n=5)

# Renombramos las columnas con nombres más descriptivos
df_structured = df_structured.rename(
    columns={
        0: "ip",
        1: "tstamp",
        2: "request",
        3: "status",
        4: "bytes",
        5: "user_agent"
    }
)
print(df_structured.head())

# Mapeo para renombrar las columnas con nombres más intuitivos
df_structured = df_structured.rename(columns={
    "ip": "ip_cliente",
    "tstamp": "fecha_hora",
    "request": "peticion",
    "status": "codigo_estado",
    "bytes": "bytes_transmitidos",
    "user_agent": "navegador_cliente"
})
print(df_structured.head())

# Extraer método y URL de la petición HTTP
df_structured[["metodo", "url"]] = (
    df_structured["peticion"]
    .str.strip('"')  # Eliminamos las comillas que envuelven la petición
    .str.split(" ", expand=True, n=1)  # Separamos método y URL en dos columnas
)
print(df_structured.head())