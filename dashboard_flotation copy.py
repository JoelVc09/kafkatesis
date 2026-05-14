from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from threading import Thread
from collections import deque
import plotly.graph_objs as go
import json
import statistics

# ============================================================
# COLORES DESDE CONFIG DEL PROYECTO
# ============================================================

from desktop_app.config import BG, PANEL, PANEL_2, TEXT, MUTED, ACCENT

GRID = "#28494F"
WARN = "#F6C85F"
CRITICAL = "#FF6B6B"
OK = "#8BE28B"

# ============================================================
# CONFIGURACION KAFKA
# ============================================================

TOPIC = "flotation-process-raw"

data_buffer = deque(maxlen=300)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="dashboard-process-sensors-v1"
)

# ============================================================
# CONSUMO KAFKA
# ============================================================

def consume_kafka():
    print("Esperando datos de sensores de proceso desde Kafka...")

    for message in consumer:
        data_buffer.append(message.value)
        print(
            f"Sensor recibido | sample_id={message.value.get('sample_id')} "
            f"batch_id={message.value.get('batch_id')}"
        )

Thread(target=consume_kafka, daemon=True).start()

# ============================================================
# APP DASH
# ============================================================

app = Dash(__name__)

# ============================================================
# LIMITES OPERACIONALES REFERENCIALES
# Ajustables segun tu operacion real
# ============================================================

LIMITS = {
    "ore_pulp_ph": {
        "name": "pH de pulpa",
        "unit": "pH",
        "warning_min": 8.5,
        "warning_max": 11.0,
        "critical_min": 7.5,
        "critical_max": 12.0
    },
    "ore_pulp_density": {
        "name": "Densidad de pulpa",
        "unit": "kg/cm³",
        "warning_min": 1.45,
        "warning_max": 1.90,
        "critical_min": 1.30,
        "critical_max": 2.10
    },
    "air_flow": {
        "name": "Flujo de aire",
        "unit": "Nm³/h",
        "warning_min": 180,
        "warning_max": 340,
        "critical_min": 150,
        "critical_max": 380
    },
    "level": {
        "name": "Nivel de espuma",
        "unit": "mm",
        "warning_min": 350,
        "warning_max": 750,
        "critical_min": 250,
        "critical_max": 900
    }
}

# ============================================================
# COMPONENTES VISUALES
# ============================================================

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

def section(title, description, graph_id):
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
            html.P(description, style={"color": MUTED, "marginTop": "0", "fontSize": "14px"}),
            dcc.Graph(id=graph_id)
        ]
    )

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

def add_limits(fig, limits):
    fig.add_hline(
        y=limits["warning_max"],
        line_dash="dash",
        line_color=WARN,
        annotation_text="Advertencia superior"
    )
    fig.add_hline(
        y=limits["critical_max"],
        line_dash="dot",
        line_color=CRITICAL,
        annotation_text="Crítico superior"
    )
    fig.add_hline(
        y=limits["warning_min"],
        line_dash="dash",
        line_color=WARN,
        annotation_text="Advertencia inferior"
    )
    fig.add_hline(
        y=limits["critical_min"],
        line_dash="dot",
        line_color=CRITICAL,
        annotation_text="Crítico inferior"
    )

def status_by_limits(value, limits):
    if value < limits["critical_min"] or value > limits["critical_max"]:
        return "CRÍTICO", CRITICAL

    if value < limits["warning_min"] or value > limits["warning_max"]:
        return "ADVERTENCIA", WARN

    return "NORMAL", OK

def add_batch_regions(fig, data):
    """
    Dibuja bloques transparentes por batch_id.
    Cada bloque representa una ventana operativa del proceso.
    """

    if not data:
        return

    batch_segments = []

    current_batch = data[0].get("batch_id", "SIN_BATCH")
    start_time = data[0].get("process_timestamp")
    previous_time = start_time

    # Detecta cambios de batch en el stream
    for d in data[1:]:
        batch_id = d.get("batch_id", "SIN_BATCH")
        ts = d.get("process_timestamp")

        if batch_id != current_batch:
            batch_segments.append({
                "batch_id": current_batch,
                "start": start_time,
                "end": previous_time
            })

            current_batch = batch_id
            start_time = ts

        previous_time = ts

    # Agrega el último batch
    batch_segments.append({
        "batch_id": current_batch,
        "start": start_time,
        "end": previous_time
    })

    colors = [
        "rgba(94, 224, 207, 0.10)",
        "rgba(246, 200, 95, 0.12)",
        "rgba(255, 107, 107, 0.08)",
        "rgba(139, 226, 139, 0.09)",
        "rgba(160, 130, 255, 0.10)"
    ]

    for idx, segment in enumerate(batch_segments):
        if not segment["start"] or not segment["end"]:
            continue

        fig.add_vrect(
            x0=segment["start"],
            x1=segment["end"],
            fillcolor=colors[idx % len(colors)],
            opacity=0.55,
            layer="below",
            line_width=0,
            annotation_text=segment["batch_id"],
            annotation_position="top left",
            annotation=dict(
                font_size=10,
                font_color="#D9F5F1"
            )
        )

# ============================================================
# LAYOUT
# ============================================================

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
                    "PROCESS SENSOR MONITORING",
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
                    "Dashboard de sensores del proceso de flotación",
                    style={"color": TEXT, "fontSize": "46px", "marginBottom": "8px"}
                ),
                html.P(
                    "Monitoreo operacional de reactivos, pulpa, aire y nivel por celda. "
                    "Este dashboard ayuda a detectar inestabilidad, desbalance y condiciones fuera de rango.",
                    style={"color": MUTED, "fontSize": "17px", "maxWidth": "1000px"}
                )
            ]
        ),

        html.Div(
            id="sensor-kpis",
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(5, 1fr)",
                "gap": "16px",
                "marginBottom": "24px"
            }
        ),

        section(
            "Dosificación de reactivos y flujo de pulpa",
            "Muestra la evolución de Starch Flow, Amina Flow y Ore Pulp Flow. "
            "Sirve para identificar cambios bruscos de dosificación o alimentación que pueden afectar la recuperación.",
            "reagents-flow-chart"
        ),

        section(
            "pH y densidad de pulpa",
            "Controla dos variables críticas de la pulpa. El pH afecta la química de flotación y la densidad refleja la carga sólida del proceso.",
            "ph-density-chart"
        ),

        section(
            "Flujo de aire por celda",
            "Compara el aire inyectado en las 7 celdas. Un desbalance puede afectar la generación de burbujas y la estabilidad de espuma.",
            "air-flow-chart"
        ),

        section(
            "Nivel de espuma por celda",
            "Permite evaluar la altura de espuma en cada celda. Niveles muy bajos o altos pueden indicar pérdida de control operacional.",
            "level-chart"
        ),

        section(
            "Balance actual de aire y nivel por celda",
            "Vista instantánea del último evento recibido. Ayuda a comparar rápidamente si una celda está operando distinta al resto.",
            "current-balance-chart"
        ),

        section(
            "Índice de estabilidad del proceso",
            "Combina la variabilidad del aire y del nivel. Un aumento sostenido indica operación inestable o desbalance entre celdas.",
            "stability-chart"
        ),

        dcc.Interval(id="update-interval", interval=1000, n_intervals=0)
    ]
)

# ============================================================
# CALLBACK
# ============================================================

@app.callback(
    [
        Output("sensor-kpis", "children"),
        Output("reagents-flow-chart", "figure"),
        Output("ph-density-chart", "figure"),
        Output("air-flow-chart", "figure"),
        Output("level-chart", "figure"),
        Output("current-balance-chart", "figure"),
        Output("stability-chart", "figure")
    ],
    [Input("update-interval", "n_intervals")]
)
def update_dashboard(n):

    if not data_buffer:
        empty = go.Figure()
        empty.update_layout(base_layout("Esperando datos desde Kafka...", "Valor"))
        return [], empty, empty, empty, empty, empty, empty

    data = list(data_buffer)
    latest = data[-1]

    timestamps = [d["process_timestamp"] for d in data]

    air_cols = [f"air_col{i:02d}" for i in range(1, 8)]
    level_cols = [f"level_col{i:02d}" for i in range(1, 8)]

    latest_air = [float(latest[col]) for col in air_cols]
    latest_level = [float(latest[col]) for col in level_cols]

    avg_air = round(statistics.mean(latest_air), 2)
    avg_level = round(statistics.mean(latest_level), 2)

    air_std = round(statistics.pstdev(latest_air), 2)
    level_std = round(statistics.pstdev(latest_level), 2)

    ph_status, ph_color = status_by_limits(float(latest["ore_pulp_ph"]), LIMITS["ore_pulp_ph"])
    density_status, density_color = status_by_limits(float(latest["ore_pulp_density"]), LIMITS["ore_pulp_density"])

    # ========================================================
    # KPIS
    # ========================================================

    kpis = [
        card("Batch", latest.get("batch_id", "-"), "Batch operativo"),
        card("Sample ID", latest.get("sample_id", "-"), "Último evento"),
        card("pH", round(float(latest["ore_pulp_ph"]), 2), ph_status, ph_color),
        card("Densidad", round(float(latest["ore_pulp_density"]), 3), density_status, density_color),
        card("Estabilidad", f"Aire σ {air_std} / Nivel σ {level_std}", "Variabilidad entre celdas", OK),
    ]

    # ========================================================
    # GRAFICO 1: REACTIVOS Y FLUJO
    # ========================================================

    fig_reagents = go.Figure()

    fig_reagents.add_trace(go.Scatter(
        x=timestamps,
        y=[float(d["starch_flow"]) for d in data],
        mode="lines",
        name="Starch Flow"
    ))

    fig_reagents.add_trace(go.Scatter(
        x=timestamps,
        y=[float(d["amina_flow"]) for d in data],
        mode="lines",
        name="Amina Flow"
    ))

    fig_reagents.add_trace(go.Scatter(
        x=timestamps,
        y=[float(d["ore_pulp_flow"]) for d in data],
        mode="lines",
        name="Ore Pulp Flow",
        yaxis="y2"
    ))

    fig_reagents.update_layout(
        title="Dosificación de reactivos y flujo de pulpa",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(title="Tiempo", gridcolor=GRID),
        yaxis=dict(title="Reactivos", gridcolor=GRID),
        yaxis2=dict(title="Ore Pulp Flow", overlaying="y", side="right"),
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )
    
    add_batch_regions(fig_reagents, data)

    # ========================================================
    # GRAFICO 2: PH Y DENSIDAD
    # ========================================================

    fig_ph_density = go.Figure()

    fig_ph_density.add_trace(go.Scatter(
        x=timestamps,
        y=[float(d["ore_pulp_ph"]) for d in data],
        mode="lines",
        name="pH"
    ))

    fig_ph_density.add_trace(go.Scatter(
        x=timestamps,
        y=[float(d["ore_pulp_density"]) for d in data],
        mode="lines",
        name="Density",
        yaxis="y2"
    ))

    fig_ph_density.update_layout(
        title="pH y densidad de pulpa",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(title="Tiempo", gridcolor=GRID),
        yaxis=dict(title="pH", gridcolor=GRID),
        yaxis2=dict(title="Density", overlaying="y", side="right"),
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    add_limits(fig_ph_density, LIMITS["ore_pulp_ph"])
    add_batch_regions(fig_ph_density, data)

    # ========================================================
    # GRAFICO 3: AIR FLOW POR CELDA
    # ========================================================

    fig_air = go.Figure()

    for i in range(1, 8):
        fig_air.add_trace(go.Scatter(
            x=timestamps,
            y=[float(d[f"air_col{i:02d}"]) for d in data],
            mode="lines",
            name=f"Celda {i}"
        ))

    fig_air.update_layout(base_layout("Flujo de aire por celda", "Nm³/h"))
    add_limits(fig_air, LIMITS["air_flow"])
    add_batch_regions(fig_air, data)

    # ========================================================
    # GRAFICO 4: LEVEL POR CELDA
    # ========================================================

    fig_level = go.Figure()

    for i in range(1, 8):
        fig_level.add_trace(go.Scatter(
            x=timestamps,
            y=[float(d[f"level_col{i:02d}"]) for d in data],
            mode="lines",
            name=f"Celda {i}"
        ))

    fig_level.update_layout(base_layout("Nivel de espuma por celda", "mm"))
    add_limits(fig_level, LIMITS["level"])
    add_batch_regions(fig_level, data)
    # ========================================================
    # GRAFICO 5: BALANCE ACTUAL
    # ========================================================

    fig_balance = go.Figure()

    fig_balance.add_trace(go.Bar(
        x=[f"Celda {i}" for i in range(1, 8)],
        y=latest_air,
        name="Air Flow"
    ))

    fig_balance.add_trace(go.Bar(
        x=[f"Celda {i}" for i in range(1, 8)],
        y=latest_level,
        name="Level",
        yaxis="y2"
    ))

    fig_balance.update_layout(
        title="Balance actual de aire y nivel por celda",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title="Air Flow", gridcolor=GRID),
        yaxis2=dict(title="Level", overlaying="y", side="right"),
        barmode="group",
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    # ========================================================
    # GRAFICO 6: INDICE DE ESTABILIDAD
    # ========================================================

    stability_values = []

    for d in data:
        air_values = [float(d[col]) for col in air_cols]
        level_values = [float(d[col]) for col in level_cols]

        air_variability = statistics.pstdev(air_values)
        level_variability = statistics.pstdev(level_values)

        stability_index = air_variability + (level_variability / 10)
        stability_values.append(round(stability_index, 2))

    fig_stability = go.Figure()

    fig_stability.add_trace(go.Scatter(
        x=timestamps,
        y=stability_values,
        mode="lines",
        name="Índice de estabilidad"
    ))

    fig_stability.add_hline(
        y=35,
        line_dash="dash",
        line_color=WARN,
        annotation_text="Advertencia"
    )

    fig_stability.add_hline(
        y=55,
        line_dash="dot",
        line_color=CRITICAL,
        annotation_text="Crítico"
    )

    fig_stability.update_layout(
        title="Índice de estabilidad del proceso",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(title="Tiempo", gridcolor=GRID),
        yaxis=dict(title="Índice", gridcolor=GRID),
        hovermode="x unified",
        margin=dict(l=55, r=30, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )
    
    add_batch_regions(fig_stability, data)

    return (
        kpis,
        fig_reagents,
        fig_ph_density,
        fig_air,
        fig_level,
        fig_balance,
        fig_stability
    )

# ============================================================
# EJECUCION
# ============================================================

if __name__ == "__main__":
    app.run_server(debug=True, port=8050)