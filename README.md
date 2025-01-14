# Yahoo_stocks_stream

# Description

protoc --java_out=YahooFinanceApp/src/main/java ticker.proto 


<h1>Kafka</h1>
1. Start Zookeeper
<code>D:\git\kafka\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties</code>
2. Start Kafka server
<code>D:\git\kafka\bin\windows\kafka-server-start.bat .\config\server.properties</code>
3. Create Kafka topic "stocks"
<code>D:\git\kafka\bin\windows\kafka-topics.bat --create --topic stocks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1</code>
4. View Kafka consumer
<code>D:\git\kafka\bin\windows\kafka-console-consumer.bat --topic stocks --from-beginning --bootstrap-server localhost:9092</code>

<h1>Load data into Kafka using Websocket in Java</h1>
1. Open YahooFinanceApp
2. Run the programme
3. Verify that data is loaded into above kafka consumer

<h1>Run pyspark script</h1>
1. Run spark submit script
<code>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 kafka_spark_stream.py</code>
2. Verify the console output contains data from kafka topic