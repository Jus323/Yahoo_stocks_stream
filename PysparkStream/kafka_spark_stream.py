import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, udf
from pyspark.sql.types import StringType, DoubleType, LongType, StructType, StructField
from pyspark.sql import functions as F

# 1. Create a Spark session
spark = SparkSession.builder \
    .appName("LiveStreamAnalyticsWithStockNames") \
    .getOrCreate()

# Suppress INFO logs
spark.sparkContext.setLogLevel("WARN")

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
df = raw_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

schema = StructType([
    StructField("time", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("change", DoubleType(), True),
    StructField("changePercentage", DoubleType(), True),
    StructField("dayVolume", LongType(), True)
])

parsed_df = df.select(
    col("key").alias("stock_key"),  # Include the key as a column
    from_json(col("value"), schema).alias("data")
).select("stock_key", "data.*")

# Convert timestamp for better readability
parsed_df = parsed_df.withColumn("event_time", (col("time") / 1000).cast("timestamp"))

# 5. Fetch Stock Name Dynamically from Yahoo Finance (one-time fetch for each unique stock)
def get_stock_name(ticker):
    stock = yf.Ticker(ticker)
    return stock.info.get('longName', ticker)

# 6. Create UDF for Stock Name Fetching
get_stock_name_udf = udf(get_stock_name, StringType())

# 7. Apply the UDF to add stock names to the DataFrame
stock_name_df = parsed_df.withColumn("stock_name", get_stock_name_udf(col("stock_key")))

# 8. Perform Live Analytics

# a. Real-Time Average Price per Stock
price_df = stock_name_df.groupBy("stock_name").agg(
    F.avg("price").alias("avg_price"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.last("price").alias("current_price")  # Fetches the most recent price for each stock
)

# b. Calculate Total Daily Volume
total_volume_df = stock_name_df.groupBy("stock_name").agg(
    F.sum("dayVolume").alias("total_volume")
)

# 9. Write the analytics results to the console

# Write Average Price
avg_price_query = price_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# Write Total Daily Volume
total_volume_query = total_volume_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await Termination of All Queries
avg_price_query.awaitTermination()
total_volume_query.awaitTermination()
