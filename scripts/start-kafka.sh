
# move to the kafka home
source scripts/configure-project.sh
cd $KAFKA_HOME

# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties
