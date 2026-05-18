from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from threading import Thread, Lock
from collections import deque
import plotly.graph_objs as go
import requests
import json
import statistics

from desktop_app.config import BG, PANEL, PANEL_2, TEXT, MUTED, ACCENT

# ============================================================
# COLORES
# ============================================================

GRID = "#28494F"
WARN = "#F6C85F"
CRITICAL = "#FF6B6B"
OK = "#8BE28B"

# ============================================================
# CONFIGURACION GENERAL
# ============================================================

TOPIC = "flotation-process-raw"

API_URL = "https://silica-prediction-api-1071825397985.us-central1.run.app/predict"

# Limites referenciales para interpretar la prediccion
SILICA_TARGET_MAX = 1.50
SILICA_WARNING_MAX = 2.50
SILICA_CRITICAL_MAX = 3.00

# Buffers
raw_buffer = deque(maxlen=500)
prediction_buffer = deque(maxlen=100)

lock = Lock()

current_batch_id = None
current_batch_rows = []

# ============================================================
# MAPEO ENTRE KAFKA Y API
# ============================================================

FEATURE_MAP = [
    ("Starch_Flow", "starch_flow"),
    ("Amina_Flow", "amina_flow"),
    ("Ore_Pulp_Flow", "ore_pulp_flow"),
    ("Ore_Pulp_pH", "ore_pulp_ph"),
    ("Ore_Pulp_Density", "ore_pulp_density"),

    ("Flotation_Column_01_Air_Flow", "air_col01"),
    ("Flotation_Column_02_Air_Flow", "air_col02"),
    ("Flotation_Column_03_Air_Flow", "air_col03"),
    ("Flotation_Column_04_Air_Flow", "air_col04"),
    ("Flotation_Column_05_Air_Flow", "air_col05"),
    ("Flotation_Column_06_Air_Flow", "air_col06"),
    ("Flotation_Column_07_Air_Flow", "air_col07"),

    ("Flotation_Column_01_Level", "level_col01"),
    ("Flotation_Column_02_Level", "level_col02"),
    ("Flotation_Column_03_Level", "level_col03"),
    ("Flotation_Column_04_Level", "level_col04"),
    ("Flotation_Column_05_Level", "level_col05"),
    ("Flotation_Column_06_Level", "level_col06"),
    ("Flotation_Column_07_Level", "level_col07"),
]

# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="dashboard-batch-prediction-v1"
)

# ============================================================
# FUNCIONES DE AGREGACION Y PREDICCION
# ============================================================

def safe_mean(rows, key):
    values = []

    for row in rows:
        try:
            values.append(float(row[key]))
        except Exception:
            pass

    if not values:
        return 0.0

    return statistics.mean(values)


def batch_numeric_id(batch_id, fallback_id):
    """
    Convierte BATCH_000123 en 123.
    Si no puede, usa sample_id como fallback.
    """
    digits = "".join(ch for ch in str(batch_id) if ch.isdigit())

    if digits:
        return int(digits)

    return int(fallback_id)


def get_silica_status(value):
    """
    Clasificacion simple del % de silice predicho.
    """
    if value >= SILICA_CRITICAL_MAX:
        return "CRÍTICO", CRITICAL

    if value >= SILICA_WARNING_MAX:
        return "ADVERTENCIA", WARN

    if value <= SILICA_TARGET_MAX:
        return "ÓPTIMO", OK

    return "NORMAL", ACCENT


def build_batch_instance(batch_id, rows):
    """
    Construye una sola fila para la API usando promedios del batch.
    """
    first = rows[0]
    last = rows[-1]

    instance = {
        "id": batch_numeric_id(batch_id, last.get("sample_id", 0))
    }

    for api_name, kafka_name in FEATURE_MAP:
        instance[api_name] = round(safe_mean(rows, kafka_name), 6)

    air_cols = [f"air_col{i:02d}" for i in range(1, 8)]
    level_cols = [f"level_col{i:02d}" for i in range(1, 8)]

    avg_air_by_cell = [safe_mean(rows, col) for col in air_cols]
    avg_level_by_cell = [safe_mean(rows, col) for col in level_cols]

    air_balance_std = statistics.pstdev(avg_air_by_cell) if len(avg_air_by_cell) > 1 else 0
    level_balance_std = statistics.pstdev(avg_level_by_cell) if len(avg_level_by_cell) > 1 else 0

    batch_stats = {
        "batch_id": batch_id,
        "start_timestamp": first.get("process_timestamp"),
        "end_timestamp": last.get("process_timestamp"),
        "rows_in_batch": len(rows),

        "avg_starch_flow": round(safe_mean(rows, "starch_flow"), 3),
        "avg_amina_flow": round(safe_mean(rows, "amina_flow"), 3),
        "avg_ore_pulp_flow": round(safe_mean(rows, "ore_pulp_flow"), 3),
        "avg_ph": round(safe_mean(rows, "ore_pulp_ph"), 3),
        "avg_density": round(safe_mean(rows, "ore_pulp_density"), 3),

        "avg_air": round(statistics.mean(avg_air_by_cell), 3),
        "avg_level": round(statistics.mean(avg_level_by_cell), 3),

        "air_balance_std": round(air_balance_std, 3),
        "level_balance_std": round(level_balance_std, 3),
        "stability_index": round(air_balance_std + (level_balance_std / 10), 3)
    }

    return instance, batch_stats


def call_prediction_api(instance):
    payload = {
        "instances": [instance]
    }

    response = requests.post(API_URL, json=payload, timeout=30)
    response.raise_for_status()

    result = response.json()

    prediction = float(result["predictions"][0])
    model_version = result.get("model_version", "unknown")

    return prediction, model_version


def finalize_batch(batch_id, rows):
    """
    Cuando termina un batch, se llama una sola vez a la API.
    """
    if not rows:
        return

    try:
        instance, batch_stats = build_batch_instance(batch_id, rows)
        prediction, model_version = call_prediction_api(instance)

        status, status_color = get_silica_status(prediction)

        record = {
            **batch_stats,
            "predicted_silica": round(prediction, 4),
            "model_version": model_version,
            "status": status,
            "status_color": status_color
        }

        with lock:
            prediction_buffer.append(record)

        print(
            f"Prediccion batch={batch_id} "
            f"silica={record['predicted_silica']} "
            f"status={status}"
        )

    except Exception as e:
        print(f"Error prediciendo batch {batch_id}: {e}")


def consume_kafka():
    """
    Consume eventos del proceso y agrupa por batch_id.
    La prediccion se ejecuta cuando detecta cambio de batch.
    """
    global current_batch_id
    global current_batch_rows

    print("Esperando datos para prediccion por batch...")

    for message in consumer:
        event = message.value
        batch_id = event.get("batch_id", "SIN_BATCH")

        batch_to_finalize = None
        rows_to_finalize = None

        with lock:
            raw_buffer.append(event)

            if current_batch_id is None:
                current_batch_id = batch_id
                current_batch_rows = [event]

            elif batch_id == current_batch_id:
                current_batch_rows.append(event)

            else:
                batch_to_finalize = current_batch_id
                rows_to_finalize = list(current_batch_rows)

                current_batch_id = batch_id
                current_batch_rows = [event]

        if batch_to_finalize and rows_to_finalize:
            finalize_batch(batch_to_finalize, rows_to_finalize)


Thread(target=consume_kafka, daemon=True).start()

# ============================================================
# APP DASH
# ============================================================

app = Dash(__name__)

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
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title=ytitle, gridcolor=GRID),
        hovermode="x unified",
        margin=dict(l=55, r=30, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )


def empty_figure(title):
    fig = go.Figure()
    fig.update_layout(
        title=title,
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(gridcolor=GRID)
    )
    return fig


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
                    "BATCH SILICA PREDICTION",
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
                    "Reporte predictivo de sílice por batch",
                    style={"color": TEXT, "fontSize": "46px", "marginBottom": "8px"}
                ),
                html.P(
                    "Este reporte agrupa los eventos de Kafka por batch_id, resume las variables operativas "
                    "y consulta la API de Machine Learning una sola vez por batch.",
                    style={"color": MUTED, "fontSize": "17px", "maxWidth": "1050px"}
                )
            ]
        ),

        html.Div(
            id="prediction-kpis",
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(5, 1fr)",
                "gap": "16px",
                "marginBottom": "24px"
            }
        ),

        section(
            "Predicción de % sílice por batch",
            "Muestra la evolución del porcentaje de sílice estimado para cada batch operativo. "
            "Permite identificar rápidamente batches fuera de condición.",
            "silica-trend-chart"
        ),

        section(
            "Reactivos promedio vs sílice predicha",
            "Relaciona la dosificación promedio de almidón y amina con la sílice estimada por batch.",
            "reagents-vs-silica-chart"
        ),

        section(
            "Estabilidad del proceso vs sílice predicha",
            "Compara la variabilidad de aire/nivel con la predicción de sílice. "
            "Ayuda a detectar si la inestabilidad operacional coincide con mayor sílice.",
            "stability-vs-silica-chart"
        ),

        section(
            "pH y densidad promedio por batch",
            "Muestra condiciones promedio de pulpa usadas para la predicción de cada batch.",
            "ph-density-batch-chart"
        ),

        section(
            "Últimas predicciones por batch",
            "Tabla resumen con trazabilidad del batch, modelo utilizado, predicción y estado operacional.",
            "prediction-table"
        ),

        dcc.Interval(id="update-interval", interval=1000, n_intervals=0)
    ]
)

# ============================================================
# CALLBACK
# ============================================================

@app.callback(
    [
        Output("prediction-kpis", "children"),
        Output("silica-trend-chart", "figure"),
        Output("reagents-vs-silica-chart", "figure"),
        Output("stability-vs-silica-chart", "figure"),
        Output("ph-density-batch-chart", "figure"),
        Output("prediction-table", "figure")
    ],
    [Input("update-interval", "n_intervals")]
)
def update_dashboard(n):

    with lock:
        predictions = list(prediction_buffer)
        current_batch = current_batch_id
        current_rows_count = len(current_batch_rows)

    if not predictions:
        kpis = [
            card("Batch actual", current_batch or "-", "En acumulación"),
            card("Filas batch actual", current_rows_count, "Eventos recibidos"),
            card("Predicción", "-", "Esperando cierre de batch"),
            card("Modelo", "-", "Sin llamada aún"),
            card("Estado", "PENDIENTE", "Aún no hay batch cerrado", WARN),
        ]

        empty = empty_figure("Esperando cierre del primer batch...")
        return kpis, empty, empty, empty, empty, empty

    latest = predictions[-1]

    silica_values = [p["predicted_silica"] for p in predictions]
    avg_silica = round(statistics.mean(silica_values), 3)
    max_silica = round(max(silica_values), 3)

    kpis = [
        card("Último batch", latest["batch_id"], "Último batch predicho"),
        card("Predicted SiO₂", latest["predicted_silica"], latest["status"], latest["status_color"]),
        card("Promedio SiO₂", avg_silica, "Promedio batches predichos"),
        card("Máximo SiO₂", max_silica, "Mayor sílice detectada"),
        card("Modelo", latest["model_version"], f"{latest['rows_in_batch']} filas en batch"),
    ]

    batch_ids = [p["batch_id"] for p in predictions]

    # ========================================================
    # GRAFICO 1: TENDENCIA SILICA
    # ========================================================

    fig_silica = go.Figure()

    fig_silica.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["predicted_silica"] for p in predictions],
        mode="lines+markers",
        name="Predicted Silica %"
    ))

    fig_silica.add_hline(
        y=SILICA_TARGET_MAX,
        line_dash="dash",
        line_color=OK,
        annotation_text="Objetivo"
    )

    fig_silica.add_hline(
        y=SILICA_WARNING_MAX,
        line_dash="dash",
        line_color=WARN,
        annotation_text="Advertencia"
    )

    fig_silica.add_hline(
        y=SILICA_CRITICAL_MAX,
        line_dash="dot",
        line_color=CRITICAL,
        annotation_text="Crítico"
    )

    fig_silica.update_layout(
        base_layout("Predicción de % sílice por batch", "% SiO₂")
    )

    # ========================================================
    # GRAFICO 2: REACTIVOS VS SILICA
    # ========================================================

    fig_reagents = go.Figure()

    fig_reagents.add_trace(go.Bar(
        x=batch_ids,
        y=[p["avg_starch_flow"] for p in predictions],
        name="Avg Starch Flow"
    ))

    fig_reagents.add_trace(go.Bar(
        x=batch_ids,
        y=[p["avg_amina_flow"] for p in predictions],
        name="Avg Amina Flow"
    ))

    fig_reagents.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["predicted_silica"] for p in predictions],
        mode="lines+markers",
        name="Predicted Silica",
        yaxis="y2"
    ))

    fig_reagents.update_layout(
        title="Reactivos promedio vs sílice predicha",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title="Reactivos", gridcolor=GRID),
        yaxis2=dict(title="% SiO₂", overlaying="y", side="right"),
        barmode="group",
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    # ========================================================
    # GRAFICO 3: ESTABILIDAD VS SILICA
    # ========================================================

    fig_stability = go.Figure()

    fig_stability.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["stability_index"] for p in predictions],
        mode="lines+markers",
        name="Índice estabilidad"
    ))

    fig_stability.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["predicted_silica"] for p in predictions],
        mode="lines+markers",
        name="Predicted Silica",
        yaxis="y2"
    ))

    fig_stability.update_layout(
        title="Estabilidad del proceso vs sílice predicha",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title="Índice estabilidad", gridcolor=GRID),
        yaxis2=dict(title="% SiO₂", overlaying="y", side="right"),
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    # ========================================================
    # GRAFICO 4: PH Y DENSIDAD PROMEDIO
    # ========================================================

    fig_ph_density = go.Figure()

    fig_ph_density.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["avg_ph"] for p in predictions],
        mode="lines+markers",
        name="Avg pH"
    ))

    fig_ph_density.add_trace(go.Scatter(
        x=batch_ids,
        y=[p["avg_density"] for p in predictions],
        mode="lines+markers",
        name="Avg Density",
        yaxis="y2"
    ))

    fig_ph_density.update_layout(
        title="pH y densidad promedio por batch",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        xaxis=dict(gridcolor=GRID),
        yaxis=dict(title="pH", gridcolor=GRID),
        yaxis2=dict(title="Density", overlaying="y", side="right"),
        hovermode="x unified",
        margin=dict(l=55, r=55, t=60, b=50),
        legend=dict(font=dict(color=TEXT))
    )

    # ========================================================
    # TABLA
    # ========================================================

    last_rows = predictions[-12:]

    fig_table = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[
                        "Batch",
                        "Inicio",
                        "Fin",
                        "Filas",
                        "Pred SiO₂",
                        "Estado",
                        "Modelo"
                    ],
                    fill_color=PANEL_2,
                    font=dict(color=TEXT, size=12),
                    align="left"
                ),
                cells=dict(
                    values=[
                        [p["batch_id"] for p in last_rows],
                        [p["start_timestamp"] for p in last_rows],
                        [p["end_timestamp"] for p in last_rows],
                        [p["rows_in_batch"] for p in last_rows],
                        [p["predicted_silica"] for p in last_rows],
                        [p["status"] for p in last_rows],
                        [p["model_version"] for p in last_rows],
                    ],
                    fill_color=PANEL,
                    font=dict(color=TEXT, size=11),
                    align="left"
                )
            )
        ]
    )

    fig_table.update_layout(
        title="Últimas predicciones por batch",
        paper_bgcolor=PANEL,
        plot_bgcolor=PANEL,
        font=dict(color=TEXT),
        margin=dict(l=20, r=20, t=60, b=20)
    )

    return (
        kpis,
        fig_silica,
        fig_reagents,
        fig_stability,
        fig_ph_density,
        fig_table
    )

# ============================================================
# EJECUCION
# ============================================================

if __name__ == "__main__":
    app.run_server(debug=True, use_reloader=False, port=8054)