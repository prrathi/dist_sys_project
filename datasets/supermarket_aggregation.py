from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import socket, time, threading
# https://www.kaggle.com/datasets/aditirai2607/super-market-dataset
'''
spark-submit --master <master_url> order_aggregate.py localhost <socket_host> 9999 "Corporate"
Stage 1: Filter orders where the Segment is "Corporate" (to analyze corporate customer behavior).
Stage 2: Count the number of orders for each Category within the "Corporate" segment.
'''
PORT_START = 9999
NUM_SOURCES = 3

def parse_line(line):
    """Parses a line of comma separated text."""
    key, value = line.split(",", 1)
    return (key, value)

def stage1_filter_segment(dstream, segment_filter):
    """Filters DStream based on Segment."""
    parsed = dstream.map(parse_line)

    # Filter out lines that don't have enough fields (at least 15 in this case)
    valid_parsed = parsed.filter(lambda kv: len(kv[1].split(",")) >= 15)

    filtered = valid_parsed.filter(lambda kv: segment_filter == kv[1].split(",")[7].strip())
    extracted = filtered.map(lambda kv: (kv[1].split(",")[14].strip(), (kv[1].split(",")[1].strip(), kv[1].split(",")[7].strip(), kv[1].split(",")[14].strip()))).repartition(NUM_SOURCES)
    extracted.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1"))
    return extracted

def stage2_count_categories(dstream):
    """Counts orders for each Category."""
    counts = dstream.map(lambda kv: (kv[0], 1)).reduceByKey(lambda x, y: x + y)
    counts.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 2"))
    return counts

def print_stage_output(rdd, stage_name):
    """Prints the contents of an RDD."""
    if not rdd.isEmpty():
        print(f"### {stage_name} Output ###")
        for record in rdd.collect():
            print(record)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: order_aggregate.py <master_url> <socket_host> <socket_port> <segment_filter>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    socket_host = sys.argv[2]
    segment_filter = sys.argv[4]

    sc = SparkContext(master_url, "OrderAggregate")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/checkpoint_aggregate_order")

    streams = []
    for i in range(NUM_SOURCES):
        stream = ssc.socketTextStream(socket_host, PORT_START + i)
        streams.append(stream)
    lines = ssc.union(*streams)

    stage1_output = stage1_filter_segment(lines, segment_filter)
    stage2_count_categories(stage1_output)

    ssc.start()
    ssc.awaitTermination()
