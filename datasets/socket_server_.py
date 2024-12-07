import socket
import time
HOST = "0.0.0.0"
PORT = 9999
CSV_FILE = "TrafficSigns_10000.csv"  # Path to your CSV file

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Socket server running on {HOST}:{PORT}")
    conn, addr = server.accept()
    print(f"Connected by {addr}")

    try:
        with open(CSV_FILE, "r") as file:
            for line in file:
                conn.sendall(line.encode("utf-8") + b"\n")
                time.sleep(1)  # Simulate real-time streaming
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
