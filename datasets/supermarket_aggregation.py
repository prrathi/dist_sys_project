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

def structured_order_aggregation(spark, csv_file_path, segment_filter="Corporate"):
    df = spark.readStream.schema(order_schema).csv(csv_file_path, header=True)
    filtered_df = df.filter(col("Segment") == segment_filter).repartition(NUM_SOURCES) 
    category_counts = filtered_df.groupBy("Category").count()

    query = category_counts.writeStream.outputMode("complete").format("console").option("checkpointLocation", "checkpoint_order_agg").start()

    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_order_aggregation.py <file_path> <segment_filter>")
        sys.exit(1)

    file_path = sys.argv[1]
    segment_filter = sys.argv[2]

    spark = SparkSession.builder.appName("StructuredStreamingOrderAggregation").config("spark.sql.shuffle.partitions", f"{NUM_SOURCES}").getOrCreate()

    structured_order_aggregation(spark, file_path, segment_filter)