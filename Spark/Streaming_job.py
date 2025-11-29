import os
import time
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, avg, max, min, to_timestamp, expr, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

class EnergyProject:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("EnergyFinalFixed") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        
        self.record_schema = StructType([
            StructField("Timestamp", StringType(), True),
            StructField("House_ID", StringType(), True),
            StructField("Room", StringType(), True),
            StructField("Device", StringType(), True),
            StructField("Device_Status", StringType(), True),
            StructField("Power_Usage_W", StringType(), True),
            StructField("Voltage_V", StringType(), True),
            StructField("Current_A", StringType(), True), 
            StructField("Energy_kWh", StringType(), True),
            StructField("Peak_Hours_Flag", StringType(), True),
            StructField("Sudden_Increase_Flag", StringType(), True),
            StructField("Temperature_C", StringType(), True)
        ])

    def print_device_stats(self, df, epoch_id):
        if df.count() > 0:
            print(f"\n📊 [QUERY 1] Device Statistics | Batch: {epoch_id}")
            time.sleep(1)
            df.show(truncate=False) 
            time.sleep(1)

    def print_alerts(self, df, epoch_id):
        if df.count() > 0:
            print("\n" + "!"*60)
            print(f"🚨 [QUERY 2] ALERT DETECTED (Device Identified!) | Batch: {epoch_id}")
            time.sleep(1)
            print("!"*60)
            df.show(truncate=False)
            time.sleep(1)
        else:
            print("\n NO ALERT DETECTED\n")
    
    def start_pipeline(self):
        print(">>> Starting Pipeline (Legacy Mode)...")

        # 1. Ingestion
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "topic-processed-data") \
            .option("startingOffsets", "latest") \
            .load()

        # 2. Parsing (Explode Array)
        json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_string")

        parsed_array = json_stream.select(
            from_json(col("json_string"), ArrayType(self.record_schema)).alias("data_array")
        )

        exploded_stream = parsed_array.select(explode(col("data_array")).alias("data"))
        
        flat_stream = exploded_stream.select("data.*")

        # 3. Cleaning & Timestamp Parsing
        # M/d/yyyy H:mm -> بيقبل 1/1/2025 0:00 (بفضل الـ Legacy Mode)
        # trim -> عشان لو فيه مسافة زيادة في الأول او الآخر
        clean_df = flat_stream \
            .withColumn("EventTime", to_timestamp(trim(col("Timestamp")), "M/d/yyyy H:mm")) \
            .withColumn("Power_Usage_W", col("Power_Usage_W").cast(DoubleType())) \
            .withColumn("Sudden_Increase_Flag", col("Sudden_Increase_Flag").cast(IntegerType()))

        # 4. Data Splitting & Watermarking
        measurements_df = clean_df.filter(col("Device").isNotNull()) \
            .select("EventTime", "House_ID", "Device", "Power_Usage_W") \
            .withWatermark("EventTime", "20 minutes")
        
        flags_df = clean_df.filter(col("Sudden_Increase_Flag") == 1) \
            .select("EventTime", "House_ID" , "Sudden_Increase_Flag") \
            .withWatermark("EventTime", "20 minutes")
        
        # 5. JOIN
        joined_alerts = flags_df.alias("f").join(
            measurements_df.alias("m"),
            expr("f.House_ID = m.House_ID AND f.EventTime = m.EventTime")
        ).select(
            col("f.EventTime"),
            col("f.House_ID"),
            col("m.Device"),
            col("m.Power_Usage_W"),
            col("f.Sudden_Increase_Flag")
        )
        
        joined_alerts = joined_alerts.dropDuplicates(["EventTime", "House_ID"])

        # 6. Outputs
        query1 = measurements_df \
            .groupBy("Device") \
            .agg(
                avg("Power_Usage_W").alias("Avg_Power"),
                max("Power_Usage_W").alias("Max_Consumption"),
                min("Power_Usage_W").alias("Min_Consumption"),
            ) \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(self.print_device_stats) \
            .start()

        query2 = joined_alerts.writeStream \
            .outputMode("append") \
            .foreachBatch(self.print_alerts) \
            .start()

        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    project = EnergyProject()
    project.start_pipeline()