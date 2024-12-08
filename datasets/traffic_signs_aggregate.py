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

def structured_traffic_aggregation(spark, csv_file_path, sign_post_filter="Traffic Signal Mast Arm"): # Example filter
    df = spark.readStream.schema(traffic_schema).option("mode", "DROPMALFORMED").csv(csv_file_path, header=True)

    filtered_df = df.filter(col("Sign_Post") == sign_post_filter).repartition(2)
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