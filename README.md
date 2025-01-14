# Yahoo Stocks Stream

A real-time stock data streaming application using Yahoo Finance, Apache Kafka, and PySpark. This application extracts live data from Yahoo Finance Websocket and streams real-time data into a Kafka topic. Subsequently, the PySpark programme reads the real-time data from the Kafka topic to generate real time data insights.

## Required Software
- Java Development Kit (JDK) 11 or later
- Python 3.8+
- Apache Kafka 3.5.0
- Apache Spark 3.5.2
- Apache Hadoop 3.3.5 (for winutils.exe)

### Python Packages
```bash
pip install pyspark==3.5.2
pip install yfinance
```

### Java Dependencies (managed by Maven)
- See `pom.xml` in the YahooFinanceApp directory

## Setup and Installation

### Hadoop Winutils Setup (Windows)
1. Download winutils.exe for Hadoop 3.3.5: https://github.com/cdarlint/winutils
2. Create directory structure:
```bash
C:\hadoop\bin
```
3. Place winutils.exe in C:\hadoop\bin
4. Add environment variables:
```
HADOOP_HOME=C:\hadoop
Path=%HADOOP_HOME%\bin
```

### Kafka Setup

1. Download Kafka: https://kafka.apache.org/quickstart
2. Start Zookeeper
```bash
D:\git\kafka\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

3. Start Kafka Server
```bash
D:\git\kafka\bin\windows\kafka-server-start.bat .\config\server.properties
```

4. Create Kafka Topic
```bash
D:\git\kafka\bin\windows\kafka-topics.bat --create --topic stocks \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

5. View Kafka Consumer (for monitoring)
```bash
D:\git\kafka\bin\windows\kafka-console-consumer.bat --topic stocks \
    --from-beginning \
    --bootstrap-server localhost:9092
```
![{8F5BBAE2-626A-4164-B370-6E79CD58A957}](https://github.com/user-attachments/assets/a7026bb4-8c21-42d2-adb5-50bf31331dee)

<br>

### Java: Data Loading via WebSocket

#### Protocol Buffers Generation
Protocol Buffers (protoc) is Google's language-agnostic, platform-neutral mechanism for serializing structured data. In the Yahoo Finance WebSocket, data received needs to be deserialized.
Generate Java classes from protocol buffer definition:
```bash
protoc --java_out=YahooFinanceApp/src/main/java ticker.proto
```

#### Java Application
1. Navigate to the YahooFinanceApp directory
2. Run the application
3. Verify data is being published to the Kafka consumer (using the consumer command above)
![{5BCCCA8B-7B7A-4335-A71D-43B16BE46D1C}](https://github.com/user-attachments/assets/859efd01-364d-4e0f-8df9-8f4bed89414d)

<br>

### Python: Running PySpark Stream Processing

1. Submit the Spark streaming job:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 kafka_spark_stream.py
```

2. Check the console output to verify that data is being successfully consumed from the Kafka topic
   ![{FABB9435-2B57-40F7-924C-209CF7A165AD}](https://github.com/user-attachments/assets/2129408e-5f4a-4ea8-9983-e2d4b1413fcf)
