import queue
import subprocess
import tkinter as tk
from tkinter import ttk
from datetime import datetime

from .config import *
from .process_manager import ProcessManager
from .commands import (
    start_zookeeper_cmd,
    start_kafka_cmd,
    create_topic_cmd,
    start_producer_cmd,
    start_dashboard_pro_cmd,
    start_dashboard_basic_cmd,
    start_dashboard_prediction_cmd,
    cleanup_cmd
)


class FlotationControlApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Flotation Streaming Control Center")
        self.root.geometry("1180x760")
        self.root.configure(bg=BG)

        self.log_queue = queue.Queue()
        self.pm = ProcessManager(self.log)

        self.status = {
            "zookeeper": False,
            "kafka": False,
            "topic": False,
            "dashboard": False,
            "producer": False
        }

        self.switches = {}
        self.vars = {}
        self.dashboard_buttons = {}

        self.setup_style()
        self.build()
        self.update_states()
        self.process_logs()

    # ========================================================
    # ESTILOS
    # ========================================================

    def setup_style(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure(
            "TButton",
            background=PANEL_2,
            foreground=TEXT,
            padding=12,
            font=("Segoe UI", 10, "bold")
        )

        style.map(
            "TButton",
            background=[("active", ACCENT), ("disabled", "#1b2f33")],
            foreground=[("active", BG), ("disabled", "#60777a")]
        )

    # ========================================================
    # LAYOUT PRINCIPAL
    # ========================================================

    def build(self):
        header = tk.Frame(self.root, bg=BG)
        header.pack(fill="x", padx=28, pady=(24, 12))

        tk.Label(
            header,
            text="KAFKA + PYTHON + DASH + TKINTER",
            bg=PANEL_2,
            fg=ACCENT,
            font=("Segoe UI", 10, "bold"),
            padx=16,
            pady=8
        ).pack(anchor="w")

        tk.Label(
            header,
            text="Centro de Control - Predicción de Sílice",
            bg=BG,
            fg=TEXT,
            font=("Segoe UI", 30, "bold")
        ).pack(anchor="w", pady=(14, 4))

        main = tk.Frame(self.root, bg=BG)
        main.pack(fill="both", expand=True, padx=28, pady=10)

        left = tk.Frame(main, bg=PANEL, padx=20, pady=20)
        left.pack(side="left", fill="y", padx=(0, 18))

        right = tk.Frame(main, bg=BG)
        right.pack(side="right", fill="both", expand=True)

        self.build_services(left)
        self.build_dashboards(right)
        self.build_logs(right)

    # ========================================================
    # PANEL DE SERVICIOS
    # ========================================================

    def build_services(self, parent):
        tk.Label(
            parent,
            text="Servicios",
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 18, "bold")
        ).pack(anchor="w")

        tk.Label(
            parent,
            text="Orden: ZooKeeper → Kafka → Topic → Dashboard → Producer",
            bg=PANEL,
            fg=MUTED,
            wraplength=280
        ).pack(anchor="w", pady=(4, 20))

        self.add_switch(
            parent,
            "01. Iniciar ZooKeeper",
            "zookeeper",
            self.start_zookeeper
        )

        self.add_switch(
            parent,
            "02. Iniciar Kafka Broker",
            "kafka",
            self.start_kafka
        )

        self.add_switch(
            parent,
            "03. Crear topic Kafka",
            "topic",
            self.create_topic
        )

        self.add_switch(
            parent,
            "04. Ejecutar Producer CSV",
            "producer",
            self.start_producer
        )

        ttk.Button(
            parent,
            text="DETENER TODOS",
            command=self.stop_all
        ).pack(fill="x", pady=(28, 0))

    def add_switch(self, parent, text, key, command):
        var = tk.BooleanVar(value=False)

        def toggle():
            if var.get():
                command()
            else:
                self.pm.stop(key)
                self.status[key] = False
                self.update_states()

        chk = tk.Checkbutton(
            parent,
            text=text,
            variable=var,
            command=toggle,
            bg=PANEL,
            fg=TEXT,
            selectcolor=PANEL_2,
            activebackground=PANEL,
            activeforeground=ACCENT,
            disabledforeground="#60777a",
            font=("Segoe UI", 11),
            anchor="w"
        )

        chk.pack(fill="x", pady=10)

        self.switches[key] = chk
        self.vars[key] = var

    # ========================================================
    # PANEL DASHBOARDS
    # ========================================================

    def build_dashboards(self, parent):
        panel = tk.Frame(parent, bg=PANEL, padx=20, pady=20)
        panel.pack(fill="x", pady=(0, 18))

        tk.Label(
            panel,
            text="Dashboards",
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 18, "bold")
        ).pack(anchor="w")

        tk.Label(
            panel,
            text="Disponibles después de crear el topic. Abre primero el reporte que quieras escuchar antes de ejecutar el Producer.",
            bg=PANEL,
            fg=MUTED,
            wraplength=760
        ).pack(anchor="w", pady=(0, 18))

        buttons = tk.Frame(panel, bg=PANEL)
        buttons.pack(fill="x")

        self.dashboard_buttons["pro"] = ttk.Button(
            buttons,
            text="Dashboard Profesional",
            command=self.start_dashboard_pro
        )
        self.dashboard_buttons["pro"].pack(side="left", padx=(0, 12))

        self.dashboard_buttons["basic"] = ttk.Button(
            buttons,
            text="Dashboard Básico",
            command=self.start_dashboard_basic
        )
        self.dashboard_buttons["basic"].pack(side="left", padx=(0, 12))

        self.dashboard_buttons["prediction"] = ttk.Button(
            buttons,
            text="Reporte Predictivo por Batch",
            command=self.start_dashboard_prediction
        )
        self.dashboard_buttons["prediction"].pack(side="left", padx=(0, 12))

    # ========================================================
    # PANEL LOGS
    # ========================================================

    def build_logs(self, parent):
        panel = tk.Frame(parent, bg=PANEL, padx=20, pady=20)
        panel.pack(fill="both", expand=True)

        tk.Label(
            panel,
            text="Logs",
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 18, "bold")
        ).pack(anchor="w", pady=(0, 12))

        self.log_text = tk.Text(
            panel,
            bg="#041114",
            fg=TEXT,
            insertbackground=TEXT,
            font=("Consolas", 10),
            borderwidth=0
        )

        self.log_text.pack(fill="both", expand=True)

    # ========================================================
    # CONTROL DE ESTADOS
    # ========================================================

    def update_states(self):
        self.switches["zookeeper"].config(state="normal")

        self.switches["kafka"].config(
            state="normal" if self.status["zookeeper"] else "disabled"
        )

        self.switches["topic"].config(
            state="normal" if self.status["kafka"] else "disabled"
        )

        dash_state = "normal" if self.status["topic"] else "disabled"

        self.dashboard_buttons["pro"].config(state=dash_state)
        self.dashboard_buttons["basic"].config(state=dash_state)
        self.dashboard_buttons["prediction"].config(state=dash_state)

        self.switches["producer"].config(
            state="normal" if self.status["dashboard"] else "disabled"
        )

    # ========================================================
    # LOGGING
    # ========================================================

    def log(self, msg, level="INFO"):
        now = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{now}] [{level}] {msg}\n")

    def process_logs(self):
        while not self.log_queue.empty():
            self.log_text.insert("end", self.log_queue.get())
            self.log_text.see("end")

        self.root.after(200, self.process_logs)

    # ========================================================
    # NAVEGADOR
    # ========================================================

    def open_browser(self, url):
        try:
            subprocess.Popen(["cmd.exe", "/c", "start", "", url])
        except Exception:
            subprocess.Popen(["bash", "-lc", f'python3 -m webbrowser "{url}"'])

        self.log(f"Abriendo navegador: {url}")

    # ========================================================
    # ACCIONES DE SERVICIOS
    # ========================================================

    def start_zookeeper(self):
        self.pm.start("zookeeper", start_zookeeper_cmd())
        self.status["zookeeper"] = True
        self.update_states()

    def start_kafka(self):
        self.pm.start("kafka", start_kafka_cmd())
        self.status["kafka"] = True
        self.update_states()

    def create_topic(self):
        self.pm.start("topic", create_topic_cmd())
        self.status["topic"] = True
        self.update_states()

    def start_producer(self):
        self.pm.start("producer", start_producer_cmd())
        self.status["producer"] = True
        self.update_states()

    # ========================================================
    # ACCIONES DE DASHBOARDS
    # ========================================================

    def start_dashboard_pro(self):
        self.pm.start("dashboard_pro", start_dashboard_pro_cmd())
        self.status["dashboard"] = True
        self.update_states()
        self.open_browser(DASHBOARD_PRO_URL)

    def start_dashboard_basic(self):
        self.pm.start("dashboard_basic", start_dashboard_basic_cmd())
        self.status["dashboard"] = True
        self.update_states()
        self.open_browser(DASHBOARD_BASIC_URL)

    def start_dashboard_prediction(self):
        self.pm.start("dashboard_prediction", start_dashboard_prediction_cmd())
        self.status["dashboard"] = True
        self.update_states()
        self.open_browser(DASHBOARD_PREDICTION_URL)

    # ========================================================
    # DETENER TODO
    # ========================================================

    def stop_all(self):
        self.log("Deteniendo todos los procesos...", "WARN")

        self.pm.stop_all()
        self.pm.start("cleanup", cleanup_cmd())

        for var in self.vars.values():
            var.set(False)

        for key in self.status:
            self.status[key] = False

        self.update_states()