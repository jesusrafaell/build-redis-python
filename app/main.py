import socket
import _thread

def client_handler(conn: socket): 
    response = "+PONG\r\n".encode()
    conn.send(response)
    conn.close()

def accept_connectins(server_socket):
    conn, addr = server_socket.accept()
    print(f"Connected to: {addr}")
    _thread.start_new_thread(client_handler, ( conn,))


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    try:
        while True:
            accept_connectins(server_socket)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()