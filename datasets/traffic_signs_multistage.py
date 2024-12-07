from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

def parse_line(line):
    """Parse a CSV line into a tuple."""
    parts = line.split(",")
    if len(parts) < 9:  # Handle missing fields gracefully
        parts += [""] * (9 - len(parts))
    try:
        return (parts[2].strip(), parts[3].strip(), parts[6].strip(), parts[8].strip())  # OBJECTID, Sign_Type, Sign_Post, Category
    except IndexError:
        return None

def stage1_filter_and_extract(dstream, filter_pattern):
    """Stage 1: Filter rows based on filter_pattern and extract relevant fields."""
    filtered = dstream.filter(
        lambda row: row and (
            filter_pattern in row[0] or filter_pattern in row[1] or filter_pattern in row[2] or filter_pattern in row[3]
        )
    )
    stage1_output = filtered.map(lambda row: (row[0], row[1], row[2], row[3]))  # Pass all fields to Stage 2
    return stage1_output

def stage2_filter_signpost_and_count(dstream, sign_post_filter):
    """Stage 2: Filter by Sign_Post type and count categories."""
    sign_post_filtered = dstream.filter(lambda t: t[2] == sign_post_filter)  # Filter for matching Sign_Post
    categories = sign_post_filtered.map(lambda t: (t[3], 1))  # Map by Category
    # Running count of categories
    def update_func(new_values, running_count):
        if running_count is None:
            running_count = 0
        return sum(new_values, running_count)
    running_counts = categories.updateStateByKey(update_func)
    return running_counts

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
    ssc = StreamingContext(sc, 2)  # Batch interval of 2 seconds
    ssc.checkpoint("checkpoint_dir")  # Checkpoint directory for stateful operations

    # Read data from the socket
    lines = ssc.socketTextStream(socket_host, socket_port)

    # Parse lines
    parsed = lines.map(parse_line)

    # Stage 1: Filter by pattern X and extract fields
    stage1_output = stage1_filter_and_extract(parsed, filter_pattern)

    # Stage 2: Filter by Sign_Post and count Categories
    running_counts = stage2_filter_signpost_and_count(stage1_output, sign_post_filter)

    # Debugging: Print Stage 1 and Stage 2 outputs
    print("### Stage 1 Output ###")
    stage1_output.pprint()

    print("### Stage 2 Running Counts ###")
    running_counts.pprint()

    # Start computation
    ssc.start()
    ssc.awaitTermination()
