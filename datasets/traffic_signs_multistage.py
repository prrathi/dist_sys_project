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
    return dstream.filter(
        lambda row: filter_pattern in row[1] or filter_pattern in row[3]
    ).map(lambda row: (row[0], row[1], row[2], row[3]))  # Pass filtered fields

def stage2_filter_signpost_and_count(dstream, sign_post_filter):
    """Stage 2: Filter by Sign_Post type and count categories."""
    counts = (
        dstream.filter(lambda t: t[2] == sign_post_filter)  # Filter for matching Sign_Post
        .map(lambda t: (t[3], 1))  # Map by Category
        .reduceByKey(lambda x, y: x + y)  # Count categories
    )
    return counts

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

    # Output results
    print("### Running Counts ###")
    running_counts.pprint()  # Display category counts

    # Start computation
    ssc.start()
    ssc.awaitTermination()
