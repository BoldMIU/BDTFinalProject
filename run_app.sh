sudo service hbase-master start
sudo service hbase-regionserver start
./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
./bin/kafka-server-start.sh -daemon config/server.properties
/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic expense
tail -n +2 ~/workspace/final.project/input/expense.csv | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic expense