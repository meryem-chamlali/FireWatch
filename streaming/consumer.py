from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)

# 1. Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("FireWatch-Kafka-to-HDFS")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Définition du schéma des messages JSON
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bright_ti4", DoubleType(), True),
    StructField("scan", DoubleType(), True),
    StructField("track", DoubleType(), True),
    StructField("acq_date", StringType(), True),
    StructField("acq_time", StringType(), True),
    StructField("satellite", StringType(), True),
    StructField("instrument", StringType(), True),
    StructField("confidence", StringType(), True),
    StructField("version", StringType(), True),
    StructField("bright_ti5", DoubleType(), True),
    StructField("frp", DoubleType(), True),
    StructField("daynight", StringType(), True),
    StructField("type", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("year", IntegerType(), True)
])

# 3. Lecture des données depuis Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "firms.detections")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

# 4. Conversion du JSON en colonnes
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

structured_df = (
    json_df
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

# 5. Conservation des messages valides
valid_df = (
    structured_df
    .filter(
        col("event_id").isNotNull()
        & col("year").isNotNull()
        & col("country").isNotNull()
    )
    .withColumn("ingestion_timestamp", current_timestamp())
)

# 6. Stockage dans HDFS au format Parquet
output_path = "hdfs://namenode:9000/raw/detections"
checkpoint_path = "hdfs://namenode:9000/checkpoints/firewatch-streaming"

query = (
    valid_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .option("compression", "snappy")
    .partitionBy("year", "country")
    .trigger(availableNow=True)
    .start()
)

# 7. Attendre la fin du traitement
query.awaitTermination()

print("Archivage des données dans HDFS terminé !")

spark.stop()
