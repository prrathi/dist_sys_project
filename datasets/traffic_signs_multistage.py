from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import sys

def parse_line(line):
    # Adjust indices according to the dataset schema
    parts = line.split(",")
    # Ensure we have enough columns
    while len(parts) < 4:
        parts.append("")
    objectid = parts[0].strip()
    sign_type = parts[1].strip()
    sign_post = parts[2].strip()    # For the second stage filter
    category = parts[3].strip()     # For counting
    return (objectid, sign_type, sign_post, category)

def stage1_filter_and_extract(dstream, filter_pattern):
    # Stage 1:
    # Filter lines containing `filter_pattern` in any field and output (OBJECTID, Sign_Type, Sign_Post, Category)
    filtered = dstream.filter(lambda row: filter_pattern in row[0] or filter_pattern in row[1] or filter_pattern in row[2] or filter_pattern in row[3])
    # Extract just (OBJECTID, Sign_Type, Sign_Post, Category) as a tuple
    # We keep Sign_Post and Category too, because Stage 2 needs them.
    # In RainStorm, stage 1 might only produce a subset of columns. 
    # For a closer analogy, we can imagine stage1.exe would only produce objectid, sign_type, and pass along sign_post, category for next stage.
    stage1_output = filtered.map(lambda row: (row[0], row[1], row[2], row[3]))
    return stage1_output

def stage2_filter_signpost_and_count(dstream, sign_post_filter):
    # Stage 2:
    # Filter by sign_post_filter and count by category
    sign_post_filtered = dstream.filter(lambda t: t[2] == sign_post_filter)
    # Key by category to count
    categories = sign_post_filtered.map(lambda t: (t[3], 1))
    
    def updateFunc(new_values, running_count):
        if running_count is None:
            running_count = 0
        return sum(new_values, running_count)
    
    running_counts = categories.updateStateByKey(updateFunc)
    return running_counts


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: traffic_signs_stream.py <master_url> <input_dir> <filter_pattern_X> <sign_post_type>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]
    input_dir = sys.argv[2]
    filter_pattern = sys.argv[3]       # pattern X 
    sign_post_filter = sys.argv[4]    

    conf = SparkConf().setAppName("TrafficSignsMultiStage")
    sc = SparkContext(master_url, "TrafficSignsMultiStage", conf=conf)
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("checkpoint_dir")

    # Read lines from input directory as they appear
    lines = ssc.textFileStream(input_dir)
    parsed = lines.map(parse_line)

    # Stage 1: Filter & Extract
    stage1_output = stage1_filter_and_extract(parsed, filter_pattern)
    # Print Stage 1 output to console, like RainStorm’s first stage does
    # Just OBJECTID, Sign_Type for demonstration
    stage1_output.map(lambda t: (t[0], t[1])).pprint()

    # Stage 2: Filter by Sign Post Type & Count Categories
    running_counts = stage2_filter_signpost_and_count(stage1_output, sign_post_filter)

    # Print Stage 2 output to console (running counts)
    running_counts.pprint()

    # Save outputs to files for final verification
    # For Stage 1 output:
    stage1_output.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage1_output"))
    # For Stage 2 output:
    running_counts.foreachRDD(lambda rdd: rdd.saveAsTextFile("Stage2_output"))

    ssc.start()
    ssc.awaitTermination()
