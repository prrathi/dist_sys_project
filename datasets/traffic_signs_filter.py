# traffic_signs_filter.py (One-Stage Filtering)


'''
assuming running on master?
spark-submit --master spark://fa24-cs425-5801.cs.illinois.edu:7077 traffic_signs_filter.py localhost localhost 9999 "Punched Telespar" #replace your_server_IP with your server address
'''
from pyspark import SparkContext  # Entry point for Spark functionality
from pyspark.streaming import StreamingContext  # For stream processing
import sys  # For command-line arguments

PORT_START = 9999  # Starting port for socket servers
NUM_SOURCES = 3   # Number of simulated source tasks

def parse_line(line):
    """Splits each incoming line (key,value string) into a key-value tuple."""
    key, value = line.split(",", 1)  # Split at the first comma
    return (key, value)

def filter_and_extract(dstream, filter_pattern):
    """Filters the DStream based on the pattern and extracts fields."""

    # 1. Parse incoming lines into key-value pairs
    parsed = dstream.map(parse_line)  # map applies a function to each element

    # 2. Filter key-value pairs where the value contains filter_pattern
    filtered = parsed.filter(lambda kv: filter_pattern in kv[1])  # filter keeps elements that satisfy a condition

    # 3. Extract OBJECTID and Sign_Type from the value, make OBJECTID the new key.
    extracted = filtered.map(lambda kv: ((kv[1].split(",")[2].strip()), (kv[1].split(",")[3].strip()))) 

    # 4. Repartition by OBJECTID to ensure data for the same OBJECTID goes to the same partition
    extracted = extracted.repartitionByKey(NUM_SOURCES)  # repartitionByKey shuffles data based on the key

    # 5. Print output for debugging (RDD actions are triggered on DStreams)
    extracted.foreachRDD(lambda rdd: print_stage_output(rdd, "Stage 1")) 
    return extracted

def print_stage_output(rdd, stage_name):
    """Prints the contents of an RDD."""
    if not rdd.isEmpty():  # Check if RDD is empty
        print(f"### {stage_name} Output ###")
        for record in rdd.collect():  # collect brings all data to the driver (for small datasets)
            print(record)

if __name__ == "__main__":
    if len(sys.argv) != 5:  # Check for correct number of command-line args
        print("Usage: traffic_signs_filter.py <master_url> <socket_host> <socket_port> <filter_pattern>", file=sys.stderr)
        sys.exit(-1)

    master_url = sys.argv[1]       # Spark master URL (e.g., 'local[2]', 'spark://...')
    socket_host = sys.argv[2]      # Socket server host (e.g., 'localhost')
    filter_pattern = sys.argv[4]  # Filter pattern (e.g., 'Stop')

    # Spark setup:
    sc = SparkContext(master_url, "TrafficSignsFilter")  # Create SparkContext
    sc.setLogLevel("ERROR")                                  # Set logging level
    ssc = StreamingContext(sc, 5)                        # Create StreamingContext (batch interval 5s)
    ssc.checkpoint("/tmp/checkpoint_filter")         # Set checkpoint directory for fault tolerance

    # Create multiple streams, one for each port
    streams = []
    for i in range(NUM_SOURCES):
        stream = ssc.socketTextStream(socket_host, PORT_START + i) # Create a DStream from a socket connection
        streams.append(stream)
    lines = ssc.union(*streams)  # Combine DStreams into a single DStream

    # Run the filter and extract operation:
    filter_and_extract(lines, filter_pattern)

    # Start the streaming computation:
    ssc.start()  # Start the StreamingContext
    ssc.awaitTermination() # Wait for the streaming computation to terminate