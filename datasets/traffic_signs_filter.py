# traffic_signs_filter.py (One-Stage Filtering)


'''
assuming running on master?
spark-submit --master spark://fa24-cs425-5801.cs.illinois.edu:7077 traffic_signs_filter.py localhost localhost 9999 "Punched Telespar" #replace your_server_IP with your server address
'''
from pyspark import SparkContext  # Entry point for Spark functionality
from pyspark.streaming import StreamingContext  # For stream processing
import sys  # For command-line arguments

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

PORT_START = 9999
NUM_SOURCES = 3

def parse_line(line):
    """Splits each incoming line into a key-value tuple."""
    key, value = line.split(",", 1)
    return (key, value)

def filter_and_extract(dstream, filter_pattern):
    """Filters the DStream based on the pattern and extracts fields."""
    parsed = dstream.map(parse_line)
    filtered = parsed.filter(lambda kv: filter_pattern in kv[1])
    # dont repart cuz last stage after filtering
    extracted = filtered.map(lambda kv: (kv[1].split(",")[2].strip(), kv[1].split(",")[3].strip()))
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
        print("Usage: traffic_signs_filter.py <master_url> <socket_host> <socket_port> <filter_pattern>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    socket_host = sys.argv[2]
    filter_pattern = sys.argv[4]

    sc = SparkContext(master_url, "TrafficSignsFilter")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/checkpoint_filter")

    streams = []
    for i in range(NUM_SOURCES):
        stream = ssc.socketTextStream(socket_host, PORT_START + i)
        streams.append(stream)
    lines = ssc.union(*streams)

    filter_and_extract(lines, filter_pattern)

    ssc.start()
    ssc.awaitTermination()