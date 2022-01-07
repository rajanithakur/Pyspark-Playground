import sys
import os
import findspark
import os
import pyspark
from pyspark.sql import SparkSession

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.functions import col,from_json

HADOOP_PATH = "/opt/spark-2.3.0-bin-hadoop2.7"
HADOOP_JARS_PATH = HADOOP_PATH + "/jars/"
findspark.init(HADOOP_PATH)

# os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args
os.environ["PYSPARK_PYTHON"] = './ecop27/dev27/bin/python'
os.environ["PYSPARK_DRIVER_PYTHON"] = './ecop27/dev27/bin/python'

schema = StructType([
  StructField("pageType",StringType(),True),
  StructField("pageContext",StringType(),True),
  StructField("deviceType",StringType(),True),
  StructField("details",ArrayType(StringType()),True),
  StructField("timestamp",StringType(),True),
  StructField("reqId",StringType(),True),
  StructField("betaCoeff",StringType(),True),
  ]
)

spark = SparkSession.builder.getOrCreate()

dataframe = spark.readStream.format("kafka").option("kafka.bootstrap.servers","kafka-1054180686-1-1268601033.wus.kafka-v2-sp-ad-server-prod.ms-df-messaging.prod-az-westus-23.prod.us.walmart.net:9092").option("subscribe", "sp_p13n_request_logs").option("multiLine", "true").option("startingOffsets", "latest").load().selectExpr("CAST(value AS STRING) as kafkamsgval")

dv = dataframe.withColumn("rootData",from_json(col("kafkamsgval"),schema)) \
                   .select("rootData")
mainDF= dv.select("rootData.pageType", "rootData.pageContext", "rootData.deviceType", "rootData.timestamp", "rootData.reqId","rootData.betaCoeff")
df5 = mainDF.filter(col("rootData.deviceType").like("ANDROID%"))
consoleoutput = df5.writeStream.format("console").outputMode("append").option("truncate", "false").start().awaitTermination()
