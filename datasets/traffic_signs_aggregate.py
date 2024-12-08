# traffic_signs_aggregate.py (Two-Stage Filtering and Aggregation)
'''
assuming running on master?
spark-submit --master spark://fa24-cs425-5801.cs.illinois.edu:7077 traffic_signs_filter.py localhost localhost 9999 "pattern" #replace your_server_IP with your server address
'''

from pyspark import SparkContext  # Import SparkContext
from pyspark.streaming import StreamingContext # Import StreamingContext
import sys  # Import sys module

PORT_START = 9999 # Starting port number for socket servers
NUM_SOURCES = 3 # Number of simulated sources

def parse_line(line):
    """Parses a line of comma separated text"""

    key, value = line.split(",", 1)
    return (key, value)

def stage1_filter_signpost(dstream, sign_post_filter):

    """Filters DStream based on Sign_Post type."""

    # Parse lines to Key, Value pairs
    parsed = dstream.map(parse_line)

    # Filter RDDs based on sign_post_filter in value
    filtered = parsed.filter(lambda kv: sign_post_filter == kv[1].split(",")[6].strip())

    # Extract Category, make it new key.
    extracted = filtered.map(lambda kv: ((kv[1].split(",")[8].strip()), (kv[1].split(",")[2].strip(), kv[1].split(",")[6].strip(), kv[1].split(",")[8].strip())))  # Make Category the new key
    
    # Repartition by Key
    extracted = extracted.repartitionByKey(NUM_SOURCES) # Repartition by Category

    # Print for debugging.
    extracted.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1"))

    return extracted

def stage2_count_categories(dstream):

    """Counts categories."""

    # Map each element to (Category, 1) for counting. Key is Category.
    counts = dstream.map(lambda kv: (kv[0], 1))

    # Reduce by Key (i.e. Category), summing the counts
    counts = counts.reduceByKey(lambda x, y: x + y)

    # Print for debugging.
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
        print("Usage: traffic_signs_aggregate.py <master_url> <socket_host> <socket_port> <sign_post_type>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1] # Spark Master URL
    socket_host = sys.argv[2] # Host of socket server.
    sign_post_filter = sys.argv[4] # Filter for the sign post.

    # Spark Setup: Create SparkContext, Streaming Context.
    sc = SparkContext(master_url, "TrafficSignsAggregate")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/checkpoint_aggregate")  # set Checkpoint directory for fault tolerance.

    streams = []
    for i in range(NUM_SOURCES): # create streams for each socket server.
        stream = ssc.socketTextStream(socket_host, PORT_START + i)
        streams.append(stream)
    lines = ssc.union(*streams)

    # Stage 1: Filter by Sign_Post (no initial repartition needed)
    stage1_output = stage1_filter_signpost(lines, sign_post_filter)

    # Stage 2: Count Categories
    stage2_count_categories(stage1_output)

    # Start Spark Streaming Job and wait for termination.
    ssc.start()
    ssc.awaitTermination()