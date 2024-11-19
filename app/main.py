import socket
import _thread
import sys
import time

storage = {}

config = {
    "dir": None,
    "dbfilename": None
}

def parse_cli_args():
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "--dir":
            config["dir"] = sys.argv[i + 1]
        elif sys.argv[i] == "--dbfilename":
            config["dbfilename"] = sys.argv[i + 1]

def set(key, value, px=None):
    expiration = None
    if px: 
        expiration = time.time() + px / 1000  

    storage[key] = (value, expiration)
    print(f"Key '{key}' set with value '{value}' and expiration {expiration}")

def get(key):
    if key not in storage:
        return None

    value, expiration = storage[key]
    if expiration and time.time() > expiration: 
        del storage[key]
        print(f"Key '{key}' has expired and was deleted.")
        return None

    return value

def delete_expired_keys():
    while True:
        current_time = time.time()
        keys_to_delete = [key for key, (_, exp) in storage.items() if exp and current_time > exp]
        for key in keys_to_delete:
            del storage[key]
            print(f"Key '{key}' expired and was removed.")
        time.sleep(1)  

def parse_resp(data):
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
        command = data.decode().strip().split()

        print(f"data: {data_list}")

        if data_list:
            command = data_list[0].upper()
            response_str = "OK"
            match command:
                case "PING":
                    response_str = "PONG"
                    response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
                case "ECHO":
                    response_str = data_list[-1]
                    response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
                case "SET": #create or update
                    key, value = data_list[1], data_list[2]
                    px = int(data_list[4]) if len(data_list) >= 5 and data_list[3].upper() == "PX" else None
                    set(key, value, px)
                    response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
                case "GET":
                    response_str = get(data_list[1]) 
                    if response_str == None:
                        response = f"$-1\r\n".encode()
                    else:
                        response = f"${len(response_str)}\r\n{response_str}\r\n".encode()
                case "CONFIG":
                    cfg_cmd = data_list[1].upper()
                    parameter =  data_list[2]
                    res = ""
                    if cfg_cmd == "GET":
                        res = config[parameter]

                    response = f"*2\r\n${len(parameter)}\r\n{parameter}\r\n${len(res)}\r\n{res}\r\n".encode()
                    print(response)
                case _:
                    response_str = data_list[-1]

            # if response_str == None:
                # response = f"$-1\r\n".encode()
            # else:
        conn.send(response)
    conn.close()

def accept_connectins(server_socket):
    conn, addr = server_socket.accept()
    print(f"Connected to: {addr}")
    _thread.start_new_thread(client_handler, (conn, addr))


def main():
    parse_cli_args()

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