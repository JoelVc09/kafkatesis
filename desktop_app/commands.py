import os
from .config import PROJECT_DIR, KAFKA_HOME, KAFKA_TOPIC, CONDA_PYTHON


# ============================================================
# UTILIDADES
# ============================================================

def script_path(name):
    """
    Retorna la ruta absoluta de un script dentro de /scripts.
    Valida que exista antes de ejecutarlo.
    """
    path = os.path.join(PROJECT_DIR, "scripts", name)

    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el script: {path}")

    return path


def bash(command):
    """
    Construye un comando bash ejecutado desde la raíz del proyecto.
    """
    return ["bash", "-lc", f'cd "{PROJECT_DIR}" && {command}']


# ============================================================
# COMANDOS DE SERVICIOS BASE
# ============================================================

def start_zookeeper_cmd():
    """
    Inicia ZooKeeper.
    """
    return bash(f'bash "{script_path("start_zookeeper.sh")}"')


def start_kafka_cmd():
    """
    Inicia Kafka Broker.
    """
    return bash(f'bash "{script_path("start_kafka.sh")}"')


def create_topic_cmd():
    """
    Crea el topic Kafka del proyecto.
    """
    return bash(f'bash "{script_path("create_topic_min.sh")}"')


def start_producer_cmd():
    """
    Ejecuta el producer que lee el CSV y publica eventos a Kafka.
    """
    return bash(f"{CONDA_PYTHON} producer_flotation.py")


# ============================================================
# COMANDOS DE DASHBOARDS
# ============================================================

def start_dashboard_pro_cmd():
    """
    Dashboard profesional / principal.
    Normalmente asociado al puerto 8052.
    """
    return bash(f"{CONDA_PYTHON} dashboard_flotation_pro.py")


def start_dashboard_basic_cmd():
    """
    Dashboard básico o de sensores de proceso.
    Normalmente asociado al puerto 8050.
    """
    return bash(f"{CONDA_PYTHON} dashboard_flotation.py")


def start_dashboard_prediction_cmd():
    """
    Reporte predictivo de sílice por batch.
    Normalmente asociado al puerto 8054.
    """
    return bash(f"{CONDA_PYTHON} dashboard_batch_prediction.py")


def start_digital_twin_stream_cmd():
    """
    Puente Kafka -> Server-Sent Events para DigitalTwinFlotacion.html.
    Normalmente asociado al puerto 8765.
    """
    return bash(f"{CONDA_PYTHON} flotation_stream_server.py")


# ============================================================
# COMANDO DE LIMPIEZA TOTAL
# ============================================================

def cleanup_cmd():
    """
    Detiene producer, dashboards, Kafka, ZooKeeper,
    elimina el topic y libera puertos usados por el sistema.
    """

    return [
        "bash",
        "-lc",
        f'''
        echo "Eliminando topic Kafka si existe..."
        "{KAFKA_HOME}/bin/kafka-topics.sh" \
            --delete \
            --topic {KAFKA_TOPIC} \
            --bootstrap-server localhost:9092 || true

        echo "Matando producer..."
        pkill -f producer_flotation.py || true

        echo "Matando dashboards..."
        pkill -f dashboard_flotation.py || true
        pkill -f dashboard_flotation_pro.py || true
        pkill -f dashboard_process_sensors.py || true
        pkill -f dashboard_equipment_health.py || true
        pkill -f dashboard_batch_prediction.py || true
        pkill -f flotation_stream_server.py || true

        echo "Liberando puertos..."
        for port in 8050 8051 8052 8053 8054 8765 9092 2181; do
            fuser -k $port/tcp || true
        done

        echo "Deteniendo Kafka y ZooKeeper..."
        bash "{script_path("stop_kafka.sh")}" || true

        echo "Limpieza finalizada."
        '''
    ]
