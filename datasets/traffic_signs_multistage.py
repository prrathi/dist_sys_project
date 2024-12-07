from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

def parse_line(line):
    """Parse a CSV line into a tuple."""
    parts = line.split(",")
    if len(parts) < 9:  # Handle missing fields gracefully
        parts += [""] * (9 - len(parts))
    return (parts[2].strip(), parts[3].strip(), parts[6].strip(), parts[8].strip())  # OBJECTID, Sign_Type, Sign_Post, Category

def stage1_filter_and_extract(dstream, filter_pattern):
    """Stage 1: Filter rows based on filter_pattern and extract relevant fields."""
    filtered = dstream.filter(
        lambda row: filter_pattern in row[1] or filter_pattern in row[3]
    ).map(lambda row: (row[0], row[1], row[2], row[3]))
    filtered.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1"))
    return filtered

def stage2_filter_signpost_and_count(dstream, sign_post_filter):
    """Stage 2: Filter by Sign_Post type and count categories."""
    filtered = dstream.filter(lambda t: t[2] == sign_post_filter)  # Filter for matching Sign_Post
    counts = filtered.map(lambda t: (t[3], 1)).reduceByKey(lambda x, y: x + y)  # Count categories
    counts.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 2"))
    return counts

def print_stage_output(rdd, stage_name):
    """Print the processed data in the current stage."""
    if not rdd.isEmpty():
        print(f"### {stage_name} Output ###")
        for record in rdd.collect():
            print(record)

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: traffic_signs_stream_socket.py <master_url> <socket_host> <socket_port> <filter_pattern_X> <sign_post_type>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    socket_host = sys.argv[2]
    socket_port = int(sys.argv[3])
    filter_pattern = sys.argv[4]       # Pattern X to filter rows
    sign_post_filter = sys.argv[5]    # Sign_Post type (e.g., 'Punched Telespar')

    # Spark configuration
    sc = SparkContext(master_url, "TrafficSignsSocket")
    sc.setLogLevel("ERROR")  # Minimal logging
    ssc = StreamingContext(sc, 5)  # Batch interval of 5 seconds
    ssc.checkpoint("/tmp/checkpoint_dir")  # Checkpoint directory for stateful operations

    # Read data from the socket
    lines = ssc.socketTextStream(socket_host, socket_port)

    # Parse lines
    parsed = lines.map(parse_line)

    # Stage 1: Filter by pattern X and extract fields
    stage1_output = stage1_filter_and_extract(parsed, filter_pattern)

    # Stage 2: Filter by Sign_Post and count Categories
    running_counts = stage2_filter_signpost_and_count(stage1_output, sign_post_filter)

    # Start computation
    ssc.start()
    ssc.awaitTermination()
