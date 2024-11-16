
source scripts/configure-project.sh

$KAFKA_HOME/bin/kafka-topics.sh \
    --create \
    --topic water-quality \
    --bootstrap-server localhost:9092 
