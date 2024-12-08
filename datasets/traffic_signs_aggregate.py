# traffic_signs_aggregate.py (Two-Stage Filtering and Aggregation)
'''
assuming running on master?
spark-submit   --master spark://fa24-cs425-5801.cs.illinois.edu:7077  traffic_signs_aggregate.py spark://fa24-cs425-5801.cs.illinois.edu:7077 f
a24-cs425-5801.cs.illinois.edu 9999 "Streetlight"
'''

from pyspark import SparkContext  # Import SparkContext
from pyspark.streaming import StreamingContext # Import StreamingContext
import sys  # Import sys module
import socket

SHUTDOWN_FLAG = "SHUTDOWN"
PORT_START = 9999
NUM_SOURCES = 3


def send_shutdown_signal(host, port):
    """Sends a shutdown signal to the specified socket server."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(SHUTDOWN_FLAG.encode("utf-8"))
            print(f"Sent shutdown signal to {host}:{port}")
    except Exception as e:
        print(f"Error sending shutdown signal to {host}:{port}: {e}")

def parse_line(line):
    """Parses a line of comma separated text."""
    key, value = line.split(",", 1)
    return (key, value)

def stage1_filter_signpost(dstream, sign_post_filter):
    """Filters DStream based on Sign_Post type."""
    parsed = dstream.map(parse_line)

    # Filter out lines that don't have enough fields
    valid_parsed = parsed.filter(lambda kv: len(kv[1].split(",")) >= 7)

    filtered = valid_parsed.filter(lambda kv: sign_post_filter == kv[1].split(",")[6].strip())
    extracted = filtered.map(lambda kv: (kv[1].split(",")[8].strip(), (kv[1].split(",")[2].strip(), kv[1].split(",")[6].strip(), kv[1].split(",")[8].strip()))).repartition(NUM_SOURCES)
    extracted.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1"))
    return extracted

def stage2_count_categories(dstream):
    """Counts categories."""
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
        print("Usage: traffic_signs_aggregate.py <master_url> <socket_host> <socket_port> <sign_post_filter>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    socket_host = sys.argv[2]
    sign_post_filter = sys.argv[4]

    sc = SparkContext(master_url, "TrafficSignsAggregate")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/checkpoint_aggregate")

    streams = []
    for i in range(NUM_SOURCES):
        stream = ssc.socketTextStream(socket_host, PORT_START + i)
        streams.append(stream)
    lines = ssc.union(*streams)

    stage1_output = stage1_filter_signpost(lines, sign_post_filter)
    stage2_count_categories(stage1_output)

    ssc.start()
    ssc.awaitTermination()

    for i in range(NUM_SOURCES):
        send_shutdown_signal(socket_host, PORT_START + i)