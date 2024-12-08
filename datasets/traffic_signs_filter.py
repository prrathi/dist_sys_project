# traffic_signs_filter.py (One-Stage Filtering)


'''
assuming running on master?
spark-submit --master spark://fa24-cs425-5801.cs.illinois.edu:7077 traffic_signs_filter.py localhost localhost 9999 "Punched Telespar" #replace your_server_IP with your server address
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys


traffic_schema = StructType([
        StructField("X", DoubleType(), True),
        StructField("Y", DoubleType(), True),
        StructField("OBJECTID", IntegerType(), True),
        StructField("Sign_Type", StringType(), True),
        StructField("Size_", StringType(), True),
        StructField("Supplement", StringType(), True),
        StructField("Sign_Post", StringType(), True),
        StructField("Year_Insta", StringType(), True), # Could also be IntegerType if data is clean.
        StructField("Category", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("MUTCD", StringType(), True),
        StructField("Ownership", StringType(), True),
        StructField("FACILITYID", StringType(), True),  # Might be IntegerType, adjust if needed
        StructField("Schools", StringType(), True),
        StructField("Location_Adjusted", StringType(), True),
        StructField("Replacement_Zone", StringType(), True), # Might be IntegerType, adjust if needed
        StructField("Sign_Text", StringType(), True),
        StructField("Set_ID", IntegerType(), True),
        StructField("FieldVerifiedDate", StringType(), True), # Might be TimestampType or DateType, but parsing strings is safer.
        StructField("GlobalID", StringType(), True)
    ])

NUM_SOURCES = 3

def structured_traffic_filter(spark, csv_file_path, filter_pattern="Streetname"):  # Example filter
    df = spark.readStream.schema(traffic_schema).option("mode", "DROPMALFORMED").csv(csv_file_path, header=True)

    filtered_df = df.filter(col("Sign_Type").contains(filter_pattern)).repartition(2)
    extracted_df = filtered_df.select("OBJECTID", "Sign_Type")

    query = extracted_df.writeStream.outputMode("append").format("console").option("checkpointLocation", "checkpoint_traffic_filter").start()
    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_traffic_filter.py <file_path> <filter_pattern>")
        sys.exit(1)

    file_path = sys.argv[1]
    filter_pattern = sys.argv[2]

    spark = SparkSession.builder.appName("StructuredStreamingTrafficFilter").config("spark.sql.shuffle.partitions", f"{NUM_SOURCES}").getOrCreate()
    structured_traffic_filter(spark, file_path, filter_pattern)