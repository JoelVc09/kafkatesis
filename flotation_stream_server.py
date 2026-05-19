import argparse
import json
import queue
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer, KafkaException


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flotation-process-raw"
HOST = "127.0.0.1"
PORT = 8765

clients = set()
clients_lock = threading.Lock()
latest_record = None
latest_lock = threading.Lock()
stop_event = threading.Event()


def publish(record):
    global latest_record
    payload = json.dumps(record, ensure_ascii=False)

    with latest_lock:
        latest_record = record

    with clients_lock:
        stale_clients = []
        for client_queue in clients:
            try:
                client_queue.put_nowait(payload)
            except queue.Full:
                stale_clients.append(client_queue)

        for client_queue in stale_clients:
            clients.discard(client_queue)


def consume_kafka(broker, topic):
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "digital-twin-html-bridge",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])
    print(f"Escuchando Kafka | broker={broker} topic={topic}")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                publish(json.loads(msg.value().decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"Mensaje Kafka ignorado: {exc}")
    except KafkaException as exc:
        print(f"No se pudo consumir Kafka: {exc}")
    finally:
        consumer.close()


class StreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}")

    def send_cors_headers(self, content_type):
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_json({"ok": True, "topic": KAFKA_TOPIC})
            return

        if self.path == "/latest":
            with latest_lock:
                record = latest_record
            self.send_json(record or {})
            return

        if self.path != "/events":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.stream_events()

    def send_json(self, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def stream_events(self):
        client_queue = queue.Queue(maxsize=20)
        with clients_lock:
            clients.add(client_queue)

        self.send_response(200)
        self.send_cors_headers("text/event-stream; charset=utf-8")
        self.end_headers()

        with latest_lock:
            initial = latest_record
        if initial:
            client_queue.put(json.dumps(initial, ensure_ascii=False))

        try:
            self.wfile.write(b": conectado\n\n")
            self.wfile.flush()

            while not stop_event.is_set():
                try:
                    payload = client_queue.get(timeout=15)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with clients_lock:
                clients.discard(client_queue)


def main():
    parser = argparse.ArgumentParser(description="Puente Kafka -> SSE para DigitalTwinFlotacion.html")
    parser.add_argument("--broker", default=KAFKA_BROKER)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", default=PORT, type=int)
    args = parser.parse_args()

    def shutdown(_signum=None, _frame=None):
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer_thread = threading.Thread(
        target=consume_kafka,
        args=(args.broker, args.topic),
        daemon=True,
    )
    consumer_thread.start()

    server = ThreadingHTTPServer((args.host, args.port), StreamHandler)
    server.timeout = 1
    print(f"SSE listo en http://{args.host}:{args.port}/events")

    try:
        while not stop_event.is_set():
            server.handle_request()
    finally:
        server.server_close()
        time.sleep(0.2)


if __name__ == "__main__":
    main()
