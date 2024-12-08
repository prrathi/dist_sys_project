from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import socket
import sys, time, threading

# https://www.kaggle.com/datasets/aditirai2607/super-market-dataset
'''
spark-submit --master <master_url> order_filter.py localhost <socket_host> 9999 "West"
Filter orders where the Region is "West" and extract the Order ID and Sales amount for further analysis of sales in the West region.
'''

PORT_START = 9999
NUM_SOURCES = 3

def parse_line(line):
    """Splits each incoming line into a key-value tuple."""
    key, value = line.split(",", 1)
    return (key, value)

def filter_and_extract(dstream, region_filter):
    """Filters by Region and extracts Order ID and Sales."""
    parsed = dstream.map(parse_line)

    # Filter out lines that don't have enough fields (at least 18 here)
    valid_parsed = parsed.filter(lambda kv: len(kv[1].split(",")) >= 18)

    filtered = valid_parsed.filter(lambda kv: region_filter == kv[1].split(",")[12].strip())
    extracted = filtered.map(lambda kv: (kv[1].split(",")[1].strip(), kv[1].split(",")[17].strip())) # .repartition(NUM_SOURCES) not doing this cuz state 
    extracted.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1"))
    return extracted

def print_stage_output(rdd, stage_name):
    """Prints the contents of an RDD."""
    if not rdd.isEmpty():
        print(f"### {stage_name} Output ###")
        for record in rdd.collect():
            print(record)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: order_filter.py <master_url> <socket_host> <socket_port> <region_filter>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    socket_host = sys.argv[2]
    region_filter = sys.argv[4]

    sc = SparkContext(master_url, "OrderFilter")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/checkpoint_filter_order")

    streams = []
    for i in range(NUM_SOURCES):
        stream = ssc.socketTextStream(socket_host, PORT_START + i)
        streams.append(stream)
    lines = ssc.union(*streams)

    filter_and_extract(lines, region_filter)

    ssc.start()
    ssc.awaitTermination()