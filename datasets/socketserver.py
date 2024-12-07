import socket
import time

# Configuration
HOST = "0.0.0.0"  # Listen on all interfaces (for master node)
PORT = 9999        # Port for the socket server
CSV_FILE = "TrafficSigns_10000.csv"  # Path to your CSV file
SLEEP_INTERVAL = 1  # Seconds between sending each line (simulates streaming)

def send_csv_data(file_path, conn):
    """Send the content of a CSV file line by line."""
    try:
        with open(file_path, "r") as file:
            for line in file:
                conn.sendall(line.encode("utf-8"))  # Send each line
                time.sleep(SLEEP_INTERVAL)        # Simulate streaming
        print(f"Finished sending data from {file_path}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen(1)
        print(f"Socket server running on {HOST}:{PORT}")
        
        conn, addr = server.accept()  # Accept a connection
        print(f"Connected by {addr}")
        
        send_csv_data(CSV_FILE, conn)  # Send the CSV data
