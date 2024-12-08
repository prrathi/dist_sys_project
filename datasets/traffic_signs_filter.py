# traffic_signs_filter.py (One-Stage Filtering)


'''
assuming running on master?
spark-submit --master spark://fa24-cs425-5801.cs.illinois.edu:7077 traffic_signs_filter.py localhost localhost 9999 "Punched Telespar" #replace your_server_IP with your server address
'''
from pyspark import SparkContext  # Entry point for Spark functionality
from pyspark.streaming import StreamingContext  # For stream processing
import sys  # For command-line arguments
import time
import socket
import threading

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

def stop_on_inactivity():
    start_time = time.time()  # Record initial time
    while True:
        time.sleep(10)  # Check every 10 seconds

        # Get the number of received records in the last timeout duration.
        # If using a receiver-based stream.
        received_records = ssc.sparkContext.accumulator(0)
        lines.foreachRDD(lambda rdd: received_records.add(rdd.count()))

        elapsed_time = time.time() - start_time

        if elapsed_time > timeout_duration and received_records.value == 0:
            print("No new data received for", timeout_duration, "seconds. Stopping Spark Streaming.")
            ssc.stop(stopSparkContext=False, stopGraceFully=True)
            break
    return

def parse_line(line):
    """Splits each incoming line into a key-value tuple."""
    key, value = line.split(",", 1)
    return (key, value)

def filter_and_extract(dstream, filter_pattern):
    """Filters the DStream based on the pattern and extracts fields."""
    parsed = dstream.map(parse_line)

    # Filter out lines that don't have enough fields (at least 4 for this script)
    valid_parsed = parsed.filter(lambda kv: len(kv[1].split(",")) >= 4)

    filtered = valid_parsed.filter(lambda kv: filter_pattern in kv[1])
    extracted = filtered.map(lambda kv: (kv[1].split(",")[2].strip(), kv[1].split(",")[3].strip())).repartition(NUM_SOURCES)
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



    # Start a separate thread to monitor inactivity
    timeout_duration = 10
    inactivity_thread = threading.Thread(target=stop_on_inactivity)
    inactivity_thread.start()

    ssc.start()
    ssc.awaitTermination()

    for i in range(NUM_SOURCES):
        send_shutdown_signal(socket_host, PORT_START + i)