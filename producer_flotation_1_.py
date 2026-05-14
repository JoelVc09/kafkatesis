from confluent_kafka import Producer
import pandas as pd
import json
import time

# ============================================================
# CONFIGURACION KAFKA
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flotation-process-raw"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

# ============================================================
# LECTURA DEL CSV
# ============================================================

CSV_PATH = "./input/datos_simulacion_sensores.csv"

data = pd.read_csv(CSV_PATH)

# ============================================================
# LIMPIEZA BASICA DE FECHA
# ============================================================

# Convierte la columna date a formato datetime.
# dayfirst=True porque tus fechas vienen como DD/MM/YYYY.
data["date"] = pd.to_datetime(data["date"], dayfirst=True, errors="coerce")

# Elimina filas donde la fecha no se pudo interpretar.
data = data.dropna(subset=["date"])

# Ordena los datos por fecha real del dataset.
data = data.sort_values("date").reset_index(drop=True)

# ============================================================
# CREACION DE TIMESTAMP SIMULADO CADA 20 SEGUNDOS
# ============================================================

# Como muchas filas tienen la misma hora, generamos un timestamp continuo.
# Esto simula que los sensores publican datos cada 20 segundos.
data["process_timestamp"] = pd.date_range(
    start=data["date"].min(),
    periods=len(data),
    freq="20s"
)

# ============================================================
# COLUMNAS DE SENSORES QUE SE PUBLICARAN EN KAFKA
# ============================================================

sensor_columns = [
    "Starch Flow",
    "Amina Flow",
    "Ore Pulp Flow",
    "Ore Pulp pH",
    "Ore Pulp Density",

    "Flotation Column 01 Air Flow",
    "Flotation Column 02 Air Flow",
    "Flotation Column 03 Air Flow",
    "Flotation Column 04 Air Flow",
    "Flotation Column 05 Air Flow",
    "Flotation Column 06 Air Flow",
    "Flotation Column 07 Air Flow",

    "Flotation Column 01 Level",
    "Flotation Column 02 Level",
    "Flotation Column 03 Level",
    "Flotation Column 04 Level",
    "Flotation Column 05 Level",
    "Flotation Column 06 Level",
    "Flotation Column 07 Level"
]

# ============================================================
# CONVERSION DE VALORES A NUMERICOS
# ============================================================

# Convierte las columnas de sensores a numero.
# Si encuentra errores, los convierte a NaN.
for col in sensor_columns:
    data[col] = pd.to_numeric(data[col], errors="coerce")

# Elimina filas con sensores incompletos.
data = data.dropna(subset=sensor_columns).reset_index(drop=True)

# ============================================================
# FILTROS BASICOS DE CALIDAD DE DATOS
# ============================================================

# Estos filtros evitan publicar valores absurdos al dashboard.
# Puedes ajustar los rangos segun tu criterio metalurgico.
data = data[
    (data["Ore Pulp pH"] >= 0) &
    (data["Ore Pulp pH"] <= 14) &
    (data["Ore Pulp Density"] >= 1) &
    (data["Ore Pulp Density"] <= 3)
].reset_index(drop=True)

# ============================================================
# CALLBACK DE ENTREGA
# ============================================================

def delivery_report(err, msg):
    """
    Confirma si Kafka recibio correctamente el mensaje.
    """
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(
            f"Mensaje enviado | topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

# ============================================================
# ENVIO DE REGISTROS A KAFKA
# ============================================================

def send_records_to_kafka(df):
    """
    Recorre el dataframe fila por fila.
    Cada fila representa un snapshot del proceso.
    """
    for index, row in df.iterrows():

        # Simulacion acelerada.
        # Para demo usamos 1 segundo.
        # En un caso real seria cada 20 segundos.
        time.sleep(1)

        # Construimos el evento JSON que viajara por Kafka.
        record = {
            "sample_id": int(index),

            # Timestamp simulado ordenado cada 20 segundos.
            "process_timestamp": row["process_timestamp"].strftime("%Y-%m-%d %H:%M:%S"),

            # Variables globales del proceso.
            "starch_flow": float(row["Starch Flow"]),
            "amina_flow": float(row["Amina Flow"]),
            "ore_pulp_flow": float(row["Ore Pulp Flow"]),
            "ore_pulp_ph": float(row["Ore Pulp pH"]),
            "ore_pulp_density": float(row["Ore Pulp Density"]),

            # Aire por celda.
            "air_col01": float(row["Flotation Column 01 Air Flow"]),
            "air_col02": float(row["Flotation Column 02 Air Flow"]),
            "air_col03": float(row["Flotation Column 03 Air Flow"]),
            "air_col04": float(row["Flotation Column 04 Air Flow"]),
            "air_col05": float(row["Flotation Column 05 Air Flow"]),
            "air_col06": float(row["Flotation Column 06 Air Flow"]),
            "air_col07": float(row["Flotation Column 07 Air Flow"]),

            # Nivel por celda.
            "level_col01": float(row["Flotation Column 01 Level"]),
            "level_col02": float(row["Flotation Column 02 Level"]),
            "level_col03": float(row["Flotation Column 03 Level"]),
            "level_col04": float(row["Flotation Column 04 Level"]),
            "level_col05": float(row["Flotation Column 05 Level"]),
            "level_col06": float(row["Flotation Column 06 Level"]),
            "level_col07": float(row["Flotation Column 07 Level"])
        }

        # Enviamos el mensaje a Kafka.
        producer.produce(
            KAFKA_TOPIC,
            key=str(record["sample_id"]),
            value=json.dumps(record),
            callback=delivery_report
        )

        # Procesa callbacks pendientes.
        producer.poll(0)

        print(f"Publicado sample_id={record['sample_id']} timestamp={record['process_timestamp']}")

    # Asegura que todos los mensajes pendientes se entreguen.
    producer.flush()

# ============================================================
# EJECUCION PRINCIPAL
# ============================================================

if __name__ == "__main__":
    print(f"Registros listos para publicar: {len(data)}")
    print(f"Enviando datos al topic: {KAFKA_TOPIC}")
    send_records_to_kafka(data)