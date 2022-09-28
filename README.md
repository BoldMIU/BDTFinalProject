
CS523 BDT final project

Spark streaming processing module for static csv dataset (Provided by https://www.kaggle.com/) with Kafka and stored into HBase database. 

Used technologies: 
 -Cloudera Quickstart VM 5.13.0
 -Java 8
 -HBase v1.2.0
 -Kafka v0.10.2.2
 -Hadoop v2.6.0
 -Spark v2.4.3

Start HBase:
1. sudo service hbase-regionserver start
2. sudo service hbase-master start

Start Kafka:
1. ./bin/kafka-server-start.sh -daemon config/server.properties 
2. ./bin/zookeeper-server-start.sh -daemon config/zookeeper.properties  
3. ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic expense  

Produce message to topic:

tail -n +2  ~/workspace/final.project/input/expense.csv | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic expense

Run Spark Streaming project form Eclipse or 

How to check result in HBase database:

>$hbase shell
>scan 'expense_db'

