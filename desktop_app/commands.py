import os
from .config import PROJECT_DIR, KAFKA_HOME, KAFKA_TOPIC, CONDA_PYTHON

def script_path(name):
    path = os.path.join(PROJECT_DIR, "scripts", name)

    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el script: {path}")

    return path

def bash(command):
    return ["bash", "-lc", f'cd "{PROJECT_DIR}" && {command}']

def start_zookeeper_cmd():
    return bash(f'bash "{script_path("start_zookeeper.sh")}"')

def start_kafka_cmd():
    return bash(f'bash "{script_path("start_kafka.sh")}"')

def create_topic_cmd():
    return bash(f'bash "{script_path("create_topic_min.sh")}"')

def start_producer_cmd():
    return bash(f"{CONDA_PYTHON} producer_flotation.py")

def start_dashboard_pro_cmd():
    return bash(f"{CONDA_PYTHON} dashboard_flotation_pro.py")

def start_dashboard_basic_cmd():
    return bash(f"{CONDA_PYTHON} dashboard_flotation.py")

def cleanup_cmd():
    return [
        "bash",
        "-lc",
        f'''
        "{KAFKA_HOME}/bin/kafka-topics.sh" --delete --topic {KAFKA_TOPIC} --bootstrap-server localhost:9092 || true
        pkill -f producer_flotation.py || true
        pkill -f dashboard_flotation.py || true
        pkill -f dashboard_flotation_pro.py || true
        for port in 8052 8051 9092 2181; do fuser -k $port/tcp || true; done
        bash "{script_path("stop_kafka.sh")}" || true
        '''
    ]