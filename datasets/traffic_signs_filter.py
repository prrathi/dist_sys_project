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
        StructField("Row ID", IntegerType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country/Region", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State/Province", StringType(), True),
        StructField("Postal Code", IntegerType(), True),
        StructField("Region", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])

NUM_SOURCES = 3

def structured_traffic_filter(spark, csv_file_path, filter_pattern="Stop"):
    # ... (Same traffic_schema as before)

    df = spark.readStream.schema(traffic_schema).csv(csv_file_path, header=True)

    filtered_df = df.filter(col("Sign Type").contains(filter_pattern)).repartition(NUM_SOURCES) #
    extracted_df = filtered_df.select("OBJECTID", "Sign Type")

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