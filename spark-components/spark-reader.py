# Set environment variable for SPARK_HOME, HADOOP_HOME and include spark directory in PATH
import os

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
                    .appName("Kafka Reader") \
                    .config("spark.local.dir", "/opt/spark/spark-tmp") \
                    .getOrCreate()