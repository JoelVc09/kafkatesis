from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from threading import Thread
from collections import deque
import plotly.graph_objs as go
import json
import os

# ============================================================
# COLORES DESDE desktop_app/config.py
# ============================================================

from desktop_app.config import BG, PANEL, PANEL_2, TEXT, MUTED, ACCENT

GRID = "#28494F"
WARN = "#F6C85F"
CRITICAL = "#FF6B6B"
OK = "#8BE28B"

# ============================================================
# CONFIGURACION
# ============================================================

TOPIC = "flotation-process-raw"
LIMITS_PATH = os.path.join("config", "equipment_limits.json")

data_buffer = deque(maxlen=300)

with open(LIMITS_PATH, "r", encoding="utf-8") as f:
    LIMITS = json.load(f)

# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="dashboard-equipment-health-v1"
)

def consume_kafka():
    print("Esperando datos de salud mecánica desde Kafka...")

    for message in consumer:
        data_buffer.append(message.value)
        print(
            f"Equipo recibido | sample_id={message.value.get('sample_id')} "
            f"batch_id={message.value.get('batch_id')}"
        )

Thread(target=consume_kafka, daemon=True).start()

# ============================================================
# APP
# ============================================================

app = Dash(__name__)

def base_layout(title, ytitle):
    return dict(
        title=title,
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(title="Tiempo", gridcolor=GRID),
        yaxis=dict(title=ytitle, gridcolor=GRID),
        hovermode="x unified",
        margin=dict(l=55, r=30, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

def card(title, value, subtitle="", color=TEXT):
    return html.Div(
        style={
            "backgroundColor": PANEL,
            "padding": "18px",
            "borderRadius": "18px",
            "border": f"1px solid {GRID}",
            "boxShadow": "0 10px 25px rgba(0,0,0,0.25)"
        },
        children=[
            html.Div(title, style={"color": MUTED, "fontSize": "13px"}),
            html.Div(str(value), style={"color": color, "fontSize": "28px", "fontWeight": "bold"}),
            html.Div(subtitle, style={"color": MUTED, "fontSize": "12px"})
        ]
    )

def section(title, desc, graph_id):
    return html.Div(
        style={
            "backgroundColor": PANEL,
            "border": f"1px solid {GRID}",
            "borderRadius": "22px",
            "padding": "20px",
            "marginBottom": "22px"
        },
        children=[
            html.H3(title, style={"color": TEXT, "marginBottom": "4px"}),
            html.P(desc, style={"color": MUTED, "marginTop": "0"}),
            dcc.Graph(id=graph_id)
        ]
    )

app.layout = html.Div(
    style={
        "backgroundColor": BG,
        "minHeight": "100vh",
        "padding": "28px",
        "fontFamily": "Arial"
    },
    children=[
        html.Div(
            style={
                "background": "linear-gradient(135deg, #071B1F 0%, #12383D 100%)",
                "padding": "34px",
                "borderRadius": "28px",
                "border": f"1px solid {GRID}",
                "marginBottom": "24px"
            },
            children=[
                html.Div(
                    "EQUIPMENT HEALTH MONITORING",
                    style={
                        "display": "inline-block",
                        "backgroundColor": PANEL_2,
                        "color": ACCENT,
                        "padding": "10px 18px",
                        "borderRadius": "22px",
                        "fontWeight": "bold"
                    }
                ),
                html.H1(
                    "Dashboard general de salud de celdas",
                    style={"color": TEXT, "fontSize": "48px"}
                ),
                html.P(
                    "Monitoreo de temperatura, vibración, voltaje y nivel de aceite por celda de flotación.",
                    style={"color": MUTED, "fontSize": "17px"}
                )
            ]
        ),

        html.Div(
            id="health-kpis",
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(5, 1fr)",
                "gap": "16px",
                "marginBottom": "24px"
            }
        ),

        section(
            "Temperatura de motores por celda",
            "Permite detectar sobrecalentamiento en los motores asociados a cada celda.",
            "motor-temperature-chart"
        ),

        section(
            "Vibración de motores por celda",
            "Permite detectar desbalance, desgaste mecánico o posible falla incipiente.",
            "motor-vibration-chart"
        ),

        section(
            "Voltaje por celda",
            "Permite verificar estabilidad eléctrica de alimentación en los equipos.",
            "voltage-chart"
        ),

        section(
            "Nivel de aceite por celda",
            "Permite controlar lubricación y prevenir operación con bajo nivel de aceite.",
            "oil-level-chart"
        ),

        section(
            "Estado actual por celda",
            "Resumen del último evento recibido desde Kafka para comparar rápidamente las 7 celdas.",
            "current-health-chart"
        ),

        dcc.Interval(id="update-interval", interval=1000, n_intervals=0)
    ]
)

# ============================================================
# FUNCIONES DE GRAFICO
# ============================================================

def add_limit_lines(fig, metric):
    limits = LIMITS[metric]

    fig.add_hline(
        y=limits["warning_max"],
        line_dash="dash",
        line_color=WARN,
        annotation_text="Límite superior advertencia"
    )

    fig.add_hline(
        y=limits["critical_max"],
        line_dash="dot",
        line_color=CRITICAL,
        annotation_text="Límite superior crítico"
    )

    if limits["warning_min"] > 0:
        fig.add_hline(
            y=limits["warning_min"],
            line_dash="dash",
            line_color=WARN,
            annotation_text="Límite inferior advertencia"
        )

    if limits["critical_min"] > 0:
        fig.add_hline(
            y=limits["critical_min"],
            line_dash="dot",
            line_color=CRITICAL,
            annotation_text="Límite inferior crítico"
        )

def get_status(value, metric):
    limits = LIMITS[metric]

    if value < limits["critical_min"] or value > limits["critical_max"]:
        return "CRÍTICO", CRITICAL

    if value < limits["warning_min"] or value > limits["warning_max"]:
        return "ADVERTENCIA", WARN

    return "NORMAL", OK

def make_metric_chart(data, metric, title):
    timestamps = [d["process_timestamp"] for d in data]

    fig = go.Figure()

    for cell in range(1, 8):
        col = f"{metric}_col{cell:02d}"

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=[float(d[col]) for d in data],
            mode="lines",
            name=f"Celda {cell}"
        ))

    unit = LIMITS[metric]["unit"]

    fig.update_layout(base_layout(title, unit))
    add_limit_lines(fig, metric)

    return fig

# ============================================================
# CALLBACK
# ============================================================

@app.callback(
    [
        Output("health-kpis", "children"),
        Output("motor-temperature-chart", "figure"),
        Output("motor-vibration-chart", "figure"),
        Output("voltage-chart", "figure"),
        Output("oil-level-chart", "figure"),
        Output("current-health-chart", "figure")
    ],
    [Input("update-interval", "n_intervals")]
)
def update_dashboard(n):

    if not data_buffer:
        empty = go.Figure()
        empty.update_layout(base_layout("Esperando datos desde Kafka...", "Valor"))
        return [], empty, empty, empty, empty, empty

    data = list(data_buffer)
    latest = data[-1]

    # ========================================================
    # KPIs GENERALES
    # ========================================================

    total_alerts = 0
    total_critical = 0

    for metric in ["motor_temperature", "motor_vibration", "voltage", "oil_level"]:
        for cell in range(1, 8):
            value = float(latest[f"{metric}_col{cell:02d}"])
            status, _ = get_status(value, metric)

            if status == "ADVERTENCIA":
                total_alerts += 1

            if status == "CRÍTICO":
                total_critical += 1

    general_status = "NORMAL"
    general_color = OK

    if total_alerts > 0:
        general_status = "ADVERTENCIA"
        general_color = WARN

    if total_critical > 0:
        general_status = "CRÍTICO"
        general_color = CRITICAL

    kpis = [
        card("Batch", latest.get("batch_id", "-"), "Batch operativo"),
        card("Sample ID", latest.get("sample_id", "-"), "Último evento"),
        card("Timestamp", latest.get("process_timestamp", "-"), "Tiempo de proceso"),
        card("Alertas", total_alerts, "Variables fuera de rango", WARN if total_alerts else OK),
        card("Estado general", general_status, "Salud actual de celdas", general_color),
    ]

    # ========================================================
    # GRAFICOS POR VARIABLE
    # ========================================================

    fig_temp = make_metric_chart(
        data,
        "motor_temperature",
        "Temperatura de motores por celda"
    )

    fig_vib = make_metric_chart(
        data,
        "motor_vibration",
        "Vibración de motores por celda"
    )

    fig_voltage = make_metric_chart(
        data,
        "voltage",
        "Voltaje por celda"
    )

    fig_oil = make_metric_chart(
        data,
        "oil_level",
        "Nivel de aceite por celda"
    )

    # ========================================================
    # ESTADO ACTUAL
    # ========================================================

    fig_current = go.Figure()

    for metric in ["motor_temperature", "motor_vibration", "voltage", "oil_level"]:
        values = [
            float(latest[f"{metric}_col{cell:02d}"])
            for cell in range(1, 8)
        ]

        fig_current.add_trace(go.Bar(
            x=[f"Celda {cell}" for cell in range(1, 8)],
            y=values,
            name=LIMITS[metric]["title"]
        ))

    fig_current.update_layout(
        title="Estado actual de salud por celda",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title="Valor", gridcolor=GRID),
        barmode="group",
        hovermode="x unified",
        margin=dict(l=55, r=30, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    return kpis, fig_temp, fig_vib, fig_voltage, fig_oil, fig_current

# ============================================================
# EJECUCION
# ============================================================

if __name__ == "__main__":
    app.run_server(debug=True, port=8052)