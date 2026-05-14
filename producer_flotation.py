from confluent_kafka import Producer
import pandas as pd
import json
import time
import random
import math
from datetime import datetime

# ============================================================
# CONFIGURACION KAFKA
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flotation-process-raw"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

# ============================================================
# CONFIGURACION CSV
# ============================================================

CSV_PATH = "./input/datos_simulacion_sensores.csv"

# ============================================================
# SIMULADOR DE VARIABLES MECANICAS POR CELDA
# ============================================================

def simulate_equipment_status(sample_id, cell_number):
    """
    Simula variables externas por celda.
    No son variables metalurgicas directas, pero ayudan a monitorear
    la salud operacional de cada celda de flotacion.
    """

    base = sample_id / 20

    return {
        f"motor_temperature_col{cell_number:02d}": round(
            (
                63
                + (cell_number * 0.7)
                + math.sin(base * 0.4 + cell_number) * 4
                + random.uniform(-1.5, 1.5)
            ),
            2
        ),

        f"motor_vibration_col{cell_number:02d}": round(
            (
                1.8
                + (cell_number * 0.08)
                + math.sin(base * 0.8 + cell_number) * 0.35
                + random.uniform(-0.18, 0.18)
            ),
            3
        ),

        f"voltage_col{cell_number:02d}": round(
            (
                440
                + math.sin(base * 0.2 + cell_number) * 4
                + random.uniform(-2.5, 2.5)
            ),
            2
        ),

        f"oil_level_col{cell_number:02d}": round(
            (
                84
                - (sample_id * 0.01)
                - (cell_number * 0.25)
                + math.sin(base * 0.15 + cell_number) * 1.2
                + random.uniform(-0.8, 0.8)
            ),
            2
        )
    }

# ============================================================
# LECTURA DEL CSV
# ============================================================

data = pd.read_csv(CSV_PATH)

# ============================================================
# LIMPIEZA DE FECHA
# ============================================================

data["date"] = pd.to_datetime(data["date"], dayfirst=True, errors="coerce")
data = data.dropna(subset=["date"])
data = data.sort_values("date").reset_index(drop=True)

# ============================================================
# CREACION DE BATCH ID
# ============================================================

# Codigo corto y unico por cada ejecucion del producer.
# Formato: F + AAMMDDHHMMSS + correlativo del batch.
RUN_CODE = datetime.now().strftime("%y%m%d%H%M%S")

# Cada date original representa un bloque operativo.
# Todas las filas con la misma fecha/hora pertenecen al mismo batch.
unique_dates = data["date"].drop_duplicates().reset_index(drop=True)
batch_sequence_width = max(3, len(str(len(unique_dates))))

batch_map = {
    date_value: f"F{RUN_CODE}{idx + 1:0{batch_sequence_width}d}"
    for idx, date_value in enumerate(unique_dates)
}

data["batch_id"] = data["date"].map(batch_map)

# ============================================================
# TIMESTAMP SIMULADO CADA 20 SEGUNDOS
# ============================================================

data["process_timestamp"] = pd.date_range(
    start=data["date"].min(),
    periods=len(data),
    freq="20s"
)

# ============================================================
# COLUMNAS DE SENSORES
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
# CONVERSION NUMERICA
# ============================================================

for col in sensor_columns:
    data[col] = pd.to_numeric(data[col], errors="coerce")

data = data.dropna(subset=sensor_columns).reset_index(drop=True)

# ============================================================
# FILTROS BASICOS DE CALIDAD
# ============================================================

data = data[
    (data["Ore Pulp pH"] >= 0) &
    (data["Ore Pulp pH"] <= 14) &
    (data["Ore Pulp Density"] >= 1) &
    (data["Ore Pulp Density"] <= 3)
].reset_index(drop=True)

# ============================================================
# CALLBACK KAFKA
# ============================================================

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(
            f"Mensaje enviado | topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

# ============================================================
# ENVIO A KAFKA
# ============================================================

def send_records_to_kafka(df):

    for index, row in df.iterrows():

        time.sleep(1)

        sample_id = int(index)

        record = {
            "sample_id": sample_id,

            # Identificador del batch operativo.
            "batch_id": row["batch_id"],

            # Fecha original del dataset.
            "source_date": row["date"].strftime("%Y-%m-%d %H:%M:%S"),

            # Timestamp simulado de streaming.
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

        # Variables mecanicas simuladas por cada celda.
        for cell in range(1, 8):
            record.update(
                simulate_equipment_status(sample_id, cell)
            )

        producer.produce(
            KAFKA_TOPIC,
            key=record["batch_id"],
            value=json.dumps(record),
            callback=delivery_report
        )

        producer.poll(0)

        print(
            f"Publicado sample_id={record['sample_id']} "
            f"batch_id={record['batch_id']} "
            f"timestamp={record['process_timestamp']}"
        )

    producer.flush()

# ============================================================
# EJECUCION
# ============================================================

if __name__ == "__main__":
    print(f"Registros listos para publicar: {len(data)}")
    print(f"Codigo de ejecucion: F{RUN_CODE}")
    print(f"Batches detectados: {data['batch_id'].nunique()}")
    print(f"Topic Kafka: {KAFKA_TOPIC}")

    send_records_to_kafka(data)
