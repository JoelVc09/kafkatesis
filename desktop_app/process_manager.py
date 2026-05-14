import subprocess
import threading

class ProcessManager:
    def __init__(self, log):
        self.processes = {}
        self.log = log

    def start(self, key, command):
        if key in self.processes and self.processes[key].poll() is None:
            self.log(f"{key} ya está en ejecución.")
            return

        self.log(f"Iniciando {key}...")

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        self.processes[key] = process

        threading.Thread(
            target=self._read_output,
            args=(key, process),
            daemon=True
        ).start()

    def _read_output(self, key, process):
        for line in process.stdout:
            self.log(f"{key}: {line.strip()}")

        self.log(f"{key} finalizó.", "WARN")

    def stop(self, key):
        process = self.processes.get(key)

        if process and process.poll() is None:
            process.terminate()
            self.log(f"{key} detenido.", "WARN")
        else:
            self.log(f"{key} no está activo.", "WARN")

    def stop_all(self):
        for key in list(self.processes.keys()):
            self.stop(key)