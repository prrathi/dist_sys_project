from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys

def parse_line(line):
    """Parse a CSV line into a tuple."""
    parts = line.split(",")
    while len(parts) < 9:  # Ensure all required fields are present
        parts.append("")
    try:
        objectid = parts[2].strip()    # OBJECTID
        sign_type = parts[3].strip()   # Sign_Type
        sign_post = parts[6].strip()   # Sign_Post
        category = parts[8].strip()    # Category
        return (objectid, sign_type, sign_post, category)
    except IndexError:
        # Log invalid line for debugging
        print(f"Invalid line: {line}")
        return None

def stage1_filter_and_extract(dstream, filter_pattern):
    """Stage 1: Filter rows based on filter_pattern and extract relevant fields."""
    filtered = dstream.filter(lambda row: row and (filter_pattern in row[0] or filter_pattern in row[1] or filter_pattern in row[2] or filter_pattern in row[3]))
    # Pass all relevant fields to Stage 2
    stage1_output = filtered.map(lambda row: (row[0], row[1], row[2], row[3]))
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
    if len(sys.argv) != 5:
        print("Usage: traffic_signs_stream.py <master_url> <input_dir> <filter_pattern_X> <sign_post_type>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    input_dir = sys.argv[2]
    filter_pattern = sys.argv[3]       # Pattern X to filter rows
    sign_post_filter = sys.argv[4]     # Sign_Post type (e.g., 'Punched Telespar')

    # Spark configuration
    conf = SparkConf().setAppName("TrafficSignsMultiStage")
    sc = SparkContext(master_url, "TrafficSignsMultiStage", conf=conf)
    #sc.setLogLevel("INFO")  # Set to INFO for debugging

    # Create StreamingContext
    ssc = StreamingContext(sc, 2)  # Batch interval of 2 seconds for debugging
    ssc.checkpoint("checkpoint_dir")  # Checkpoint directory for stateful operations

    # Read lines from the input directory
    lines = ssc.textFileStream(input_dir)

    # Debugging: Print raw lines
    print("### RAW INPUT LINES ###")
    lines.pprint()

    # Parse each line of the CSV
    parsed = lines.map(parse_line)

    # Debugging: Print parsed lines
    print("### PARSED LINES ###")
    parsed.pprint()

    # Stage 1: Filter by pattern X and extract fields
    stage1_output = stage1_filter_and_extract(parsed, filter_pattern)
    # Debugging: Print Stage 1 output
    print("### STAGE 1 OUTPUT ###")
    stage1_output.pprint()

    # Stage 2: Filter by Sign_Post and count Categories
    running_counts = stage2_filter_signpost_and_count(stage1_output, sign_post_filter)
    # Debugging: Print Stage 2 output
    print("### STAGE 2 OUTPUT ###")
    running_counts.pprint()

    # Save outputs to files for verification
    stage1_output.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage1_output"))
    running_counts.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage2_output"))

    # Start computation
    ssc.start()
    ssc.awaitTermination()
