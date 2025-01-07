from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
from pyspark.sql.streaming.state import GroupState

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("IncrementalAverage") \
    .getOrCreate()

# 2. Kafka Configuration
kafka_brokers = "localhost:9092"
topic_name = "stocks"

# 3. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Decode and parse Kafka messages
df = raw_df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("time", LongType(), True)
])

parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn("event_time", (col("time") / 1000).cast("timestamp"))

# 5. Define State Schema
class StockState:
    def __init__(self, count=0, sum_price=0.0):
        self.count = count
        self.sum_price = sum_price

    def update(self, new_price):
        self.count += 1
        self.sum_price += new_price

    def get_average(self):
        return self.sum_price / self.count if self.count > 0 else 0.0

# 6. Define Stateful Update Function
def update_state(stock_id, updates, state: GroupState):
    # Retrieve or initialize state
    current_state = state.get(StockState()) if state.exists else StockState()
    
    # Update state with new data
    for row in updates:
        current_state.update(row.price)

    # Save updated state
    state.update(current_state)

    # Return the updated average
    return (stock_id, current_state.get_average(), current_state.count)

# 7. Apply Stateful Aggregation
from pyspark.sql.functions import expr

stateful_df = parsed_df.groupBy("id").applyInPandasWithState(
    update_state,
    stateSchema=StockState,
    outputMode="append"
)

# 8. Write Results
query = stateful_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
