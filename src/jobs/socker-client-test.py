import socket

def receive_data_from_socket(host='127.0.0.1', port=9999):
    # Create a client socket and connect to the server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    try:
        while True:
            data = s.recv(1024)
            if not data:
                break
            print("Received:", data.decode('utf-8'))
    except ConnectionResetError:
        print("Connection closed by server.")
    finally:
        s.close()

if __name__ == "__main__":
    receive_data_from_socket()