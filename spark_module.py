from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import shutil
import os
import glob
import pandas as pd
import time

checkpointLocation = "/home/bate/smart-retail-example/V1/spark"
outputPath = "/home/bate/smart-retail-example/V1/output/spark"

# Delete the checkpoint directory if it exists
if os.path.exists(checkpointLocation):
    shutil.rmtree(checkpointLocation)

if os.path.exists(outputPath):
    shutil.rmtree(outputPath)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Read data from Kafka topic as a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "m1_spark,m3_spark") \
    .option("startingOffsets", "earliest") \
    .load()

#ds = df \
  #.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  #.writeStream \
  #.format("kafka") \
  #.option("kafka.bootstrap.servers", "localhost:9092") \
  #.option("topic", "spark_table") \
  #.option("checkpointLocation", "/home/bate/smart-retail-example/V1/spark") \
  #.start() \
  #.awaitTermination()

df_s = df.selectExpr("CAST(value AS STRING)") 

# Define the schema for the incoming data
schema = StructType() \
    .add("person", IntegerType()) \
    .add("zone", IntegerType()) \
    .add("entry_frame", IntegerType()) \
    .add("exit_frame", IntegerType()) \
    .add("latency", FloatType())

df_f = df_s.select(from_json(col("value"), schema).alias("data")).select("data.*")

df_f.writeStream\
	.format("csv")\
    .option("path", outputPath) \
    .option("header", "true") \
	.option("mode", "append")\
	.option("checkpointLocation", checkpointLocation)\
	.start()\
	.awaitTermination(timeout=30)

# csv files in the path
files = glob.glob(outputPath + "/*.csv")
  
# defining an empty list to store 
# content
columns = ['person_id', 'zone_id', 'entry_frame', 'exit_frame']
data_frame = pd.DataFrame(columns=columns)
data_frame.set_index(['person_id', 'zone_id'], inplace=True)
content = []
  
# checking all the csv files in the 
# specified path
for filename in files:
    
    # reading content of csv file
    # content.append(filename)
    df = pd.read_csv(filename, index_col=None)
    print(df)
    content.append(df)
  
# converting content to data frame
data_frame = pd.concat(content)

data_frame.to_csv('/home/bate/smart-retail-example/V1/output/spark_output.csv')