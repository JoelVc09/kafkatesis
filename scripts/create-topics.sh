
KAFKA_HOME=/mnt/c/kafka/kafka_2.13-3.8.0

$KAFKA_HOME/bin/kafka-topics.sh \
    --create \
    --topic water-quality \
    --bootstrap-server localhost:9092 
