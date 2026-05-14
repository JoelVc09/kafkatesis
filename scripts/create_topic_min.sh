KAFKA_HOME=/mnt/c/kafka/kafka_2.13-3.8.0

$KAFKA_HOME/bin/kafka-topics.sh \
    --create \
    --topic flotation-process-raw \
    --bootstrap-server localhost:9092