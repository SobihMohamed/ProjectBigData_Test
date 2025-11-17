from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class EnergyStreamingJob:
  def __init__(self , kafka_bootstrap='localhost:9092', topic='energy_reading', db_url=None):
    #define variables
    self.kafka_bootstrap = kafka_bootstrap
    self.topic = topic
    self.db_url = db_url

    #Create spark session 
    self.SparkSession = SparkSession.builder.appName("EnergyStreaming")\
      .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1").getOrCreate()

    #create Schema
    self.Schema = StructType([
      StructField("devices",StringType(),True),
      StructField("consumption",IntegerType(),True),
      StructField("timestamp",StringType(),True),
    ])

  def start_streaming(self):
    #read from the kafka which has the value in binary
    dataFromKafka = self.SparkSession.readStream\
      .format("kafka")\
      .option("kafka.bootstrap.servers",self.kafka_bootstrap)\
      .option("subscribe",self.topic)\
      .option("startingOffsets","earliest")\
      .load()
    #and the change it to string 
    #and then change it into Schema which i Created
    dataToSavedInDataFrame = dataFromKafka.selectExpr("CAST(value AS STRING) as raw_data") \
                                .select(from_json(col("raw_data"), self.Schema).alias("data")) \
                                .select("data.*")

    # create some Analysis 
    dataToSavedInDataFrame.groupBy("devices") \
      .avg("consumption") \
      .writeStream \
      .outputMode("complete") \
      .format("console") \
      .option("checkpointLocation", "D:/spark_checkpoint") \
      .start() \
      .awaitTermination()

      
if __name__ == "__main__":
    job = EnergyStreamingJob()
    job.start_streaming()