
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from threading import Thread
from collections import deque
import plotly.graph_objs as go
import json
import time

# Configurar Kafka
TOPIC = 'water-quality'
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Estructura para almacenar datos recientes
data_buffer = deque(maxlen=100)  # Máximo de 100 mensajes almacenados

# Función para consumir datos en un hilo separado
def consume_kafka():
    for message in consumer:
        data_buffer.append(message.value)

# Iniciar el hilo del consumidor
consumer_thread = Thread(target=consume_kafka, daemon=True)
consumer_thread.start()

# Crear aplicación Dash
app = Dash(__name__)

app.layout = html.Div([
    dcc.Graph(id='real-time-graph'),
    dcc.Interval(id='update-interval', interval=1000, n_intervals=0)  # Actualización cada segundo
])

# Callback para actualizar el gráfico
@app.callback(
    Output('real-time-graph', 'figure'),
    [Input('update-interval', 'n_intervals')]
)
def update_graph(n):
    # Leer los datos del buffer
    if not data_buffer:
        return go.Figure()  # Devuelve un gráfico vacío si no hay datos
    
    # Procesar los datos del buffer
    timestamps = [d['timestamp'] for d in data_buffer]
    turbidity = [d['water_turbidity'] for d in data_buffer]
    device_ids = [d['device_id'] for d in data_buffer]
    
    figure = go.Figure()
    for device_id in set(device_ids):
        device_data = [(ts, tb) for ts, tb, d_id in zip(timestamps, turbidity, device_ids) if d_id == device_id]
        figure.add_trace(go.Scatter(x=[d[0] for d in device_data], 
                                    y=[d[1] for d in device_data],
                                    mode='lines+markers',
                                    name=f'Sensor {device_id}'))
        
    
    # Configurar etiquetas de los ejes
    figure.update_layout(
        title="Water Turbidity Over Time",  # Título del gráfico
        xaxis_title="Timestamp",           # Etiqueta del eje X
        yaxis_title="Water Turbidity",     # Etiqueta del eje Y
        legend_title="Devices"             # Título de la leyenda
    )
    
    return figure

if __name__ == '__main__':
    app.run_server(debug=True)

