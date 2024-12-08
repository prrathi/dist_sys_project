from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

order_schema = StructType([
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
        StructField("Postal Code", IntegerType(), True),  # Assuming postal code is integer, adjust if needed.
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
def structured_order_filter(spark, csv_file_path, region_filter="West"):

    df = spark.readStream.schema(order_schema).csv(csv_file_path, header=True)
    filtered_df = df.filter(col("Region") == region_filter).repartition(NUM_SOURCES) 
    extracted_df = filtered_df.select("Order ID", "Sales")

    query = extracted_df.writeStream.outputMode("append").format("console").option("checkpointLocation", "checkpoint_order_filter").start()
    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_order_filter.py <file_path> <region_filter>")
        sys.exit(1)

    file_path = sys.argv[1]
    region_filter = sys.argv[2]

    spark = SparkSession.builder.appName("StructuredStreamingOrderFilter").config("spark.sql.shuffle.partitions", f"{NUM_SOURCES}").getOrCreate()
    structured_order_filter(spark, file_path, region_filter)