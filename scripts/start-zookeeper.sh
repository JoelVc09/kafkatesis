
# move to the kafka home
source scripts/configure-project.sh
cd $KAFKA_HOME

# Start the ZooKeeper service
bin/zookeeper-server-start.sh config/zookeeper.properties
