import socket
import time
import threading
import os
import sys

# simulating sources
SHUTDOWN_FLAG = "SHUTDOWN"
HOST = "0.0.0.0"
PORT_START = 9999  # Starting port for the socket servers
NUM_SOURCES = 3   # Number of simulated source tasks



def run_server(port, start_line, end_line):
    """Runs a socket server that sends a portion of the CSV file."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, port))
        server.listen(1)
        conn, addr = server.accept()
        print(f"Socket server on port {port} connected by {addr}")

        try:
            with open(CSV_FILE, "r") as file:
                next(file)  # Skip the header row
                lines_sent = 0
                for linenumber, line in enumerate(file, 1):  # enumerate gives line number starting from 1
                    if start_line <= linenumber <= end_line:
                        key = f"file:{linenumber}"
                        value = line.strip()
                        message = f"{key},{value}\n"
                        conn.sendall(message.encode("utf-8"))
                        lines_sent += 1
                        time.sleep(0.01)  # Adjust as needed

            print(f"Socket server on port {port} finished sending {lines_sent} lines.")
            while True:
                # just busy waiting too lazy, dont want the connection to be closed till evetryhing processsed
                time.sleep(60)
        except Exception as e:
            print(f"Error in socket server on port {port}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    
    if (len(sys.argv) != 2):
        print("Usage: socket_server.py <csv_file>")
        exit(-1)
    CSV_FILE = sys.argv[1]
    try:
        with open(CSV_FILE, "r") as file:
            total_lines = sum(1 for _ in file) - 1 # Subtract header

        lines_per_source = total_lines // NUM_SOURCES
        threads = []

        for i in range(NUM_SOURCES):
            start_line = i * lines_per_source + 1
            end_line = (i + 1) * lines_per_source if i < NUM_SOURCES - 1 else total_lines  # Handle last portion

            thread = threading.Thread(target=run_server, args=(PORT_START + i, start_line, end_line))
            threads.append(thread)
            thread.start()

        for thread in threads:
          thread.join()

    except FileNotFoundError:
        print(f"Error: CSV file '{CSV_FILE}' not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")