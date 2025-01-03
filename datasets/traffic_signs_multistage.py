from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys

def parse_line(line):
    """Parse a CSV line into a tuple."""
    parts = line.split(",")
    while len(parts) < 9:  # Ensure required fields are present
        parts.append("")
    try:
        return (parts[2].strip(), parts[3].strip(), parts[6].strip(), parts[8].strip())  # OBJECTID, Sign_Type, Sign_Post, Category
    except IndexError:
        return None

def stage1_filter_and_extract(dstream, filter_pattern):
    """Stage 1: Filter rows based on filter_pattern and extract relevant fields."""
    filtered = dstream.filter(lambda row: row and (filter_pattern in row[0] or filter_pattern in row[1] or filter_pattern in row[2] or filter_pattern in row[3]))
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
    if len(sys.argv) != 5:
        print("Usage: traffic_signs_stream.py <master_url> <input_dir> <filter_pattern_X> <sign_post_type>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    input_dir = sys.argv[2]
    filter_pattern = sys.argv[3]       # Pattern X to filter rows
    sign_post_filter = sys.argv[4]     # Sign_Post type (e.g., 'Punched Telespar')

    # Spark configuration
    conf = SparkConf().setAppName("TrafficSignsWithFeedback")
    sc = SparkContext(master_url, "TrafficSignsWithFeedback", conf=conf)
    sc.setLogLevel("ERROR")  # Minimal logging

    # Create StreamingContext
    ssc = StreamingContext(sc, 2)  # Batch interval of 2 seconds
    ssc.checkpoint("checkpoint_dir")  # Checkpoint directory for stateful operations

    # Read lines from the input directory
    lines = ssc.textFileStream(input_dir)

    # Feedback: Check if data is flowing at the raw input level
    def debug_raw_lines(rdd):
        if rdd.isEmpty():
            print(">>> No new files detected in the input directory.")
        else:
            print(f">>> Raw input batch size: {rdd.count()} lines.")

    lines.foreachRDD(debug_raw_lines)

    # Parse lines into fields
    parsed = lines.map(parse_line)

    # Feedback: Parsed lines
    def debug_parsed_lines(rdd):
        if rdd.isEmpty():
            print(">>> No parsed lines in this batch.")
        else:
            print(f">>> Parsed batch size: {rdd.count()} records.")
    parsed.foreachRDD(debug_parsed_lines)

    # Stage 1: Filter by pattern X and extract fields
    stage1_output = stage1_filter_and_extract(parsed, filter_pattern)
    # Feedback: Stage 1 output
    def debug_stage1_output(rdd):
        if rdd.isEmpty():
            print(">>> Stage 1 output: No data passed through filter.")
        else:
            print(f">>> Stage 1 output batch size: {rdd.count()} records.")
            print(f"    Sample: {rdd.take(5)}")
    stage1_output.foreachRDD(debug_stage1_output)

    # Stage 2: Filter by Sign_Post and count Categories
    running_counts = stage2_filter_signpost_and_count(stage1_output, sign_post_filter)
    # Feedback: Stage 2 output
    def debug_stage2_output(rdd):
        if rdd.isEmpty():
            print(">>> Stage 2 output: No matching categories found.")
        else:
            print(f">>> Stage 2 running counts batch size: {rdd.count()} records.")
            print(f"    Sample: {rdd.take(5)}")
    running_counts.foreachRDD(debug_stage2_output)

    # Save outputs to files for verification
    stage1_output.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage1_output"))
    running_counts.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage2_output"))

    # Start computation
    ssc.start()
    ssc.awaitTermination()
