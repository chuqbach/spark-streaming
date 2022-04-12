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


class Streaming:
    def __init__(self, **config):
        self.config =

    def init_spark_session(self):
        spark = SparkSession.builder.getOrCreate()

    def readStream(self):
        spark.readStream()

    def writeStream(self):
        ...

    def transformStream(self):
