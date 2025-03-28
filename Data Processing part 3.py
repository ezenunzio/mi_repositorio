import pandas as pd
from faker import Faker

# Función para generar un DataFrame ficticio con logs de servidor
def generate_fake_logs(n):
    fake = Faker()
    logs = []
    for _ in range(n):
        ip = fake.ipv4()  # Generamos una dirección IP
        tstamp = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")  # Generamos un timestamp formateado
        method = fake.random_element(["GET", "POST", "PUT", "DELETE"])  # Seleccionamos un método HTTP aleatorio
        url = fake.uri()  # Generamos una URL aleatoria
        request = f"{method} {url}"  # Construimos la petición HTTP
        status = fake.random_int(100, 599)  # Generamos un código de estado HTTP aleatorio
        bytes_transmitted = fake.random_int(100, 10000)  # Simulamos la cantidad de bytes transmitidos
        user_agent = fake.user_agent()  # Generamos un user agent aleatorio
        logs.append(f'{ip} - {tstamp} - "{request}" - {status} - {bytes_transmitted} - "{user_agent}"')

    return pd.DataFrame(logs, columns=["log"])

# Generamos 1000 logs ficticios
df = generate_fake_logs(1000)

print(df)

# Configuramos pandas para visualizar datos sin truncar
pd.set_option('display.max_colwidth', None)
print(df.head())

# Separar la columna 'log' en múltiples columnas estructuradas
df_structured = df["log"].str.split(" - ", expand=True, n=5)

# Renombramos las columnas con nombres más descriptivos
df_structured = df_structured.rename(
    columns={
        0: "ip_cliente",
        1: "fecha_hora",
        2: "peticion",
        3: "codigo_estado",
        4: "bytes_transmitidos",
        5: "navegador_cliente"
    }
)
print(df_structured.head())

# Extraer método y URL de la petición HTTP
df_structured[["metodo", "url"]] = (
    df_structured["peticion"]
    .str.strip('"')  # Eliminamos las comillas que envuelven la petición
    .str.split(" ", expand=True, n=1)  # Separamos método y URL en dos columnas
)
print(df_structured.head())