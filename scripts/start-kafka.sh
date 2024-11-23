
KAFKA_HOME=/mnt/c/kafka/kafka_2.13-3.8.0
cd $KAFKA_HOME
# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties
