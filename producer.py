
from confluent_kafka import Producer
import pandas as pd
import json

# Define Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "water-quality"

# Kafka configuration
kafka_config = {
    'bootstrap.servers': KAFKA_BROKER,  
}

# Initialize the Kafka producer
producer = Producer(kafka_config)

# Define the Kafka topic
topic = KAFKA_TOPIC  

data = pd.read_csv('./input/sorted_streaming_data.csv')
data['SensorID'] = data['SensorID'].astype(str)


# Function to deliver messages
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

import time 

# Function to send DataFrame records to Kafka
def send_records_to_kafka(df):
    
    for _, row in df.iterrows():
        time.sleep(1)
        
        record = {
            'device_id': row['SensorID'],
            'timestamp': row['DateTime'],
            'water_turbidity': row['Turbidity']
        }
        
        # Convert the record to JSON format
        record_json = json.dumps(record)

        # Send the record to Kafka
        producer.produce(
            topic, 
            key=row['SensorID'], 
            value=record_json, 
            callback=delivery_report
        )

        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()

# Send the records from the DataFrame to Kafka
send_records_to_kafka(data)
