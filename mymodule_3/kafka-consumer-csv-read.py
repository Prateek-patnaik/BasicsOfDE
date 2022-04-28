from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,DoubleType,StringType,IntegerType
import time


import time


kafka_topic_name = "mytopic1"
kafka_output_topic_name="mytopic2"
kafka_bootstrap_servers = 'localhost:9092'
if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()

    print("Printing Schema of orders_df: ")
    df.printSchema()

    df1 = df.selectExpr("CAST(value AS STRING)")

    myschema_string=StructType()\
    .add("slno",StringType())\
    .add("Address",StringType())\
    .add("Name",StringType())\
    .add("x1",StringType())\
    .add("x2",StringType())\
    .add("x3",StringType())\
    .add("x4",StringType())\
    .add("Place",StringType())\
    .add("Device",StringType())\
    .add("Value",StringType())


    df2 = df1 \
        .select(from_json(df1.value, myschema_string) \
                .alias("table_values"))

    df3 = df2.select("table_values.*")
    df3.printSchema()



print("Stream Data Processing Application Completed.")

df4=df3.groupby("name","address")\
        .agg(sum("x1").alias("sum_x1"), \
             sum("x2").alias("sum_x2"), \
             sum("x3").alias("sum_x3"), \
             sum("x4").alias("sum_x4"))

df4.printSchema()

df4.select(to_json(struct("*")).alias("value")) \
    .selectExpr("CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic",kafka_output_topic_name) \
    .save()

# orders_agg_write_stream = df4 \
#     .writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .load()
#
# orders_agg_write_stream.awaitTermination()

# df4.select(to_json(struct("*")).alias("value")) \
#     .selectExpr("CAST(value AS STRING)")\
#     .writeStream\
#     .format("kafka")\
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
#     .option("topic", kafka_output_topic_name) \
#     .trigger(processingTime='5 seconds') \
#     .outputMode("update")\
#     .option("checkpointLocation","file:///C:/Users/prateek.patnaik/Downloads/spark-kafka/checkpoint")\
#     .start()\
#     .awaitTermination()
#
# print("Stream Data Processing Application Completed.")





