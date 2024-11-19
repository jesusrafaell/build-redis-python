import socket
import _thread

storage = {}

def parse_resp(data):
    """Parse RESP data into an array of strings."""
    lines = data.decode().split("\r\n")
    result = []

    i = 1
    while i < len(lines):
        if lines[i].startswith("$"): 
            result.append(lines[i + 1])
            i += 2
        else:
            i += 1 

    return result


def client_handler(conn: socket, addr): 
    while True:
        data = conn.recv(1024)
        if not data: 
            print(f"Connection closed by {addr}")
            break

        data_list = parse_resp(data)

        print(f"data: {data_list}")

        if data_list:
            command = data_list[0].upper()
            response_str = "OK"
            match command:
                case "PING":
                    response_str = "PONG"
                case "ECHO":
                    response_str = data_list[-1]
                    response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
                case "SET": #create or update
                    key = data_list[1]
                    value = ' '.join(data_list[2:])
                    storage[key] = value
                case "GET":
                    response_str =  storage[data_list[1]]
                case _:
                    response_str = data_list[-1]
            response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
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