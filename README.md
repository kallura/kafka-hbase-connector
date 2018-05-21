## Kafka HBase sink connector

This is a very simple Kafka sink connector to HBase

!!! Supports only String, Integer, Long, Double filed types !!!

#####Prerequisites
1. Java v1.8
2. Kafka v1.1.0
3. Maven v3.5.2

#####Installing

Start up your HBase cluster

Start Kafka
````
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties 
````
Build the project
````
mvn clean install
````
Copy the jars to ${kafka_home}/libs
````
kafka-hbase-connector-1.0-SNAPSHOT-jar-with-dependencies.jar
````
Add connector properties to ${kafka_home}/config
````
name=kafka-cdc-hbase
connector.class=org.korlenko.connector.HBaseSinkConnector
tasks.max=1
topics=power,energy,temperature
zookeeper.quorum=localhost:2181
record.parser.class=org.korlenko.parser.impl.JsonSinkRecordParser
base.compressor.class=org.apache.hadoop.hbase.coprocessor.AggregateImplementation
hbase.power.rowkey.columns=id,timestamp,power
hbase.power.rowkey.delimiter=|
hbase.power.family=power
hbase.energy.rowkey.columns=id,timestamp,energy
hbase.energy.rowkey.delimiter=|
hbase.energy.family=energy
hbase.temperature.rowkey.columns=id,timestamp,temperature
hbase.temperature.rowkey.delimiter=|
hbase.temperature.family=temperature

````
Run connector
````
 bin/connect-standalone.sh config/connect-standalone.properties config/hbase.properties
````

#####todo

Add support for all possible data types

Add tests