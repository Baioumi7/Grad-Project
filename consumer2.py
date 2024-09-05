from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Define schema based on the Kafka message structure
schema = StructType([
    StructField("date", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("weather_code", LongType(), True),
    StructField("surface_pressure", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("cloud_cover_low", DoubleType(), True),
    StructField("cloud_cover_high", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True),
    StructField("wind_direction_100m", DoubleType(), True),
    StructField("soil_temperature_28_to_100cm", DoubleType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from Kafka
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "WEATHER") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka messages as JSON
weather_df_parsed = weather_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Processing logic (here we simply print the data)
query = weather_df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()