# traffic_signs_aggregate.py (Two-Stage Filtering and Aggregation)
'''
assuming running on master?
spark-submit   --master spark://fa24-cs425-5801.cs.illinois.edu:7077  traffic_signs_aggregate.py spark://fa24-cs425-5801.cs.illinois.edu:7077 f
a24-cs425-5801.cs.illinois.edu 9999 "Streetlight"\
ignore
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

def structured_traffic_aggregation(spark, csv_file_path, sign_post_filter="MUTCD Signpost"):

    df = spark.readStream.schema(traffic_schema).csv(csv_file_path, header=True)

    filtered_df = df.filter(col("Sign Post") == sign_post_filter).repartition(NUM_SOURCES) 
    category_counts = filtered_df.groupBy("Category").count()

    query = category_counts.writeStream.outputMode("complete").format("console").option("checkpointLocation", "checkpoint_traffic_agg").start()
    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_traffic_aggregation.py <file_path> <sign_post_filter>")
        sys.exit(1)

    file_path = sys.argv[1]
    sign_post_filter = sys.argv[2]

    spark = SparkSession.builder.appName("StructuredStreamingTrafficAggregation").config("spark.sql.shuffle.partitions", f"{NUM_SOURCES}").getOrCreate()

    structured_traffic_aggregation(spark, file_path, sign_post_filter)