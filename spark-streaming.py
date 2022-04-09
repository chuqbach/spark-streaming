# Set environment variable for SPARK_HOME, HADOOP_HOME and include spark directory in PATH
import os

# Add dependencies for Spark (Kafka, Spark-Excel, etc.)
submit_args = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'
if 'PYSPARK_SUBMIT_ARGS' not in os.environ:
    os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args
else:
    os.environ['PYSPARK_SUBMIT_ARGS'] += submit_args

import findspark
findspark.init()
findspark.find()

import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
                    .appName("Kafka Structured Streaming") \
                    .config("spark.local.dir", "/opt/spark/spark-tmp") \
                    .getOrCreate()

source_df = (
            spark.readStream
                .format('kafka')
                .option("kafka.bootstrap.servers", 'localhost:9092')
                .option("subscribe", 'test')
                .option("startingOffsets", 'earliest')
                .option("maxOffsetsPerTrigger", '1')
                .option("group.id", "group01")
                .option("stopGracefullyOnShutdown", "true")
                .load()
)
#
df = source_df.selectExpr('topic', 'partition', 'offset', 'timestamp', 'CAST(key as STRING)', 'CAST(value as STRING)')

print(df.printSchema)


def loader(df, batchid):
    value_schema = spark.read.json(df.rdd.map(lambda row: row.value)).schema
    df1 = df.withColumn('parsed_col', F.from_json(F.col('value'), value_schema)) \
            .select(F.lit(batchid).alias('batchId'), 'topic', 'partition', 'offset',
                    F.expr('timestamp as processing_timestamp'), 'key', 'parsed_col.*')
    df1.write.json(mode="append", path="G:/HomeCredit/Projects/Spark Streaming/data/json")


# query = df.writeStream.format('console') \
#           .trigger(processingTime="30 seconds") \
#           .outputMode("append") \
#           .option("checkpointLocation", 'G:/HomeCredit/Projects/Spark Streaming/checkpoint/') \
#           .start()


query = (
        df.writeStream.format('json')
                    .outputMode("append")
                    .trigger(processingTime="1 seconds")
                    .option("path", "/home/chuqbach/dev/spark-streaming/data/json")
                    .option("checkpointLocation", '/home/chuqbach/dev/spark-streaming/checkpoint/')
                    .start()
)

# query = df.writeStream.outputMode("append") \
#                       .trigger(processingTime="5 seconds") \
#                       .option("checkpointLocation", 'G:/HomeCredit/Projects/Spark Streaming/checkpoint/') \
#                       .foreachBatch(loader) \
#                       .start()

query.awaitTermination()



















