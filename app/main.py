import socket


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()

    while True:
        print(f"Connected {addr}")

        data = conn.recv(1024)
        if not data:
            break
        response = "+PONG\r\n".encode()
        conn.sendall(response)

    conn.close()

if __name__ == "__main__":
    main()
