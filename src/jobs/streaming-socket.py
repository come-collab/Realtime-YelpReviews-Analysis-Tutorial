import json
import socket
import time
import pandas as pd

def send_data_over_socket(file_path, host='127.0.0.1', port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print("Server listening on port", port)

  

    while True:
        conn, addr = s.accept()
        print(f"Connection from {addr}")

        try:
            with open(file_path, 'rb') as file:
                records = []
                for line in file:
                    records.append(json.loads(line))
                    if len(records) == chunk_size:
                        chunk = pd.DataFrame(records)
                        print(chunk)
                        for record in chunk.to_dict(orient='records'):
                            serialize_data = json.dumps(record).encode('utf-8')
                            conn.send(serialize_data + b'\n')
                            time.sleep(5)  # Reduced sleep for faster testing
                        records = []
        except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected")
        finally:
            conn.close()
            print("Connection closed")


if __name__ == "__main__":
    send_data_over_socket('src/datasets/yelp_academic_dataset_review.json')


