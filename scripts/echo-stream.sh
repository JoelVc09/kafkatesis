
source scripts/configure-project.sh

$KAFKA_HOME/bin/kafka-console-consumer.sh \
    --topic water-quality \
    --from-beginning \
    --bootstrap-server localhost:9092
