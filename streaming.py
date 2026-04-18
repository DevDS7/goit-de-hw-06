import os
import sys

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from configs import kafka_config
from topic_configs import BUILDING_TOPIC, ALERTS_TOPIC

spark = (
    SparkSession.builder
    .appName("KafkaAlertsStreaming")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .option("subscribe", BUILDING_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .load()
)


json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])


clean_df = (
    df.selectExpr("CAST(value AS STRING) as value_str")
    .withColumn("json_data", from_json(col("value_str"), json_schema))
    .select(
        col("json_data.id").alias("id"),
        col("json_data.temperature").alias("temperature"),
        col("json_data.humidity").alias("humidity"),
        col("json_data.timestamp").alias("event_time")
    )
    .withColumn(
        "event_time",
        from_unixtime(col("event_time").cast("double")).cast("timestamp")
    )
)


aggregated_df = (
    clean_df
    .withWatermark("event_time", "10 seconds")
    .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
    .agg(
        round(avg("temperature"), 2).alias("t_avg"),
        round(avg("humidity"), 2).alias("h_avg"),
        max("event_time").alias("timestamp")
    )
)


alerts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("alerts_conditions.csv")
)


alerts_df.printSchema()
alerts_df.show(truncate=False)

alerts_joined_df = (
    aggregated_df.crossJoin(alerts_df)
    .filter(
        (
            # якщо є температурні межі → перевіряємо температуру
            (
                (col("temperature_min") != -999) | (col("temperature_max") != -999)
            ) &
            (
                ((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
                ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max")))
            )
        )
        |
        (
            # якщо є humidity межі → перевіряємо humidity
            (
                (col("humidity_min") != -999) | (col("humidity_max") != -999)
            ) &
            (
                ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
                ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max")))
            )
        )
    )
)


result_df = alerts_joined_df.select(
    to_json(
        struct(
            struct(
                col("window.start").cast("string").alias("start"),
                col("window.end").cast("string").alias("end")
            ).alias("window"),
            col("t_avg"),
            col("h_avg"),
            col("code").cast("string").alias("code"),
            col("message"),
            col("timestamp").cast("string").alias("timestamp")
        )
    ).alias("value")
)


console_query = (
    result_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/checkpoints-alerts-console")
    .start()
)


kafka_query = (
    result_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("topic", ALERTS_TOPIC)
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .option("checkpointLocation", "/tmp/checkpoints-alerts-kafka")
    .start()
)

spark.streams.awaitAnyTermination()