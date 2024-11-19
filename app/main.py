import socket
import _thread

def client_handler(conn: socket, addr): 
    while True:
        data = conn.recv(1024)
        if not data: 
            print(f"Connection closed by {addr}")
            break
        response = "+PONG\r\n".encode()
        conn.send(response)
    conn.close()

def accept_connectins(server_socket):
    conn, addr = server_socket.accept()
    print(f"Connected to: {addr}")
    _thread.start_new_thread(client_handler, (conn, addr))


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    print(f'Server is listing')
    try:
        while True:
            accept_connectins(server_socket)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()