import fnmatch
import os
import socket
import _thread
import struct
import sys
import time

storage = {}

config = {
    "dir": None,
    "dbfilename": None
}

def read_size(file):
    first_byte = ord(file.read(1))
    if first_byte & 0b11000000 == 0b00000000:
        return first_byte & 0b00111111
    elif first_byte & 0b11000000 == 0b01000000:
        second_byte = ord(file.read(1))
        return ((first_byte & 0b00111111) << 8) | second_byte
    elif first_byte & 0b11000000 == 0b10000000:
        size_bytes = file.read(4)
        return struct.unpack(">I", size_bytes)[0]
    else:
        raise ValueError("Unsupported size encoding")

def decode_string(file):
    length = read_size(file)
    return file.read(length).decode('utf-8', errors='ignore')

def load_rdb_file(dir, dbfilename):
    file_path = os.path.join(dir, dbfilename)
    if not os.path.exists(file_path):
        print(f"RDB file not found {dbfilename}")
        return

    try:
        with open(file_path, "rb") as file:
            header = file.read(9)
            if not header.startswith(b"REDIS"):
                raise ValueError("Invalid RDB file format")

            while True:
                marker = file.read(1)
                if not marker:
                    break

                if marker == b'\xFE':
                    db_index = read_size(file)  # Índice de la base de datos (puedes ignorarlo)
                elif marker == b'\xFB':
                    # Tamaños de tablas hash (ignorado en este ejemplo)
                    hash_table_size = read_size(file)
                    expiry_table_size = read_size(file)
                elif marker == b'\x00':
                    # Clave simple (sin expiración)
                    key = decode_string(file)
                    value = decode_string(file)
                    storage[key] = value
                elif marker == b'\xFD' or marker == b'\xFC':
                    # Clave con expiración (FD: segundos, FC: milisegundos)
                    if marker == b'\xFD':
                        expiry = struct.unpack("<I", file.read(4))[0]
                    elif marker == b'\xFC':
                        expiry = struct.unpack("<Q", file.read(8))[0]
                    key = decode_string(file)
                    value = decode_string(file)
                    storage[key] = value  # Podrías almacenar también la expiración
                elif marker == b'\xFF':
                    # Fin del archivo
                    break
                else:
                    print(f"Unknown marker: {marker}")
                    break

        print(f"Loaded {len(storage)} keys from RDB file: {list(storage.keys())}")

    except Exception as e:
        print(f"Error reading RDB: {e}")



def get_keys(pattern: str) -> list[str]:
    keys = fnmatch.filter(storage.keys(), pattern)
    return keys


def format_array_response(values: list[str]) -> bytes:
    response = f"*{len(values)}\r\n"
    for value in values:
        response += f"${len(value)}\r\n{value}\r\n"
    return response.encode()

def format_response(value: str) -> bytes:
    response = f"${len(value)}\r\n{value}\r\n"
    return response.encode()

def parse_cli_args():
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "--dir":
            config["dir"] = sys.argv[i + 1]
        elif sys.argv[i] == "--dbfilename":
            config["dbfilename"] = sys.argv[i + 1]

def set(key: str, value: str, px=None):
    expiration = None
    if px: 
        expiration = time.time() + px / 1000  

    storage[key] = (value, expiration)
    print(f"Key '{key}' set with value '{value}' and expiration {expiration}")

def get(key: str) -> str:
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

def parse_resp(data: str) -> list[str]:
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

def client_handler(conn: socket.socket, addr): 
    while True:
        data = conn.recv(1024)
        if not data: 
            print(f"Connection closed by {addr}")
            break

        data_list = parse_resp(data)
        command = data.decode().strip().split()

        print(f"data: {data_list}")

        response_str = "OK"
        if data_list:
            command = data_list[0].upper()
            match command:
                case "PING":
                    response = format_response("PONG")
                case "ECHO":
                    response = format_response(data_list[-1])
                case "SET": #create or update
                    key, value = data_list[1], data_list[2]
                    px = int(data_list[4]) if len(data_list) >= 5 and data_list[3].upper() == "PX" else None
                    set(key, value, px)
                    response = format_response(response_str)
                case "GET":
                    response_str = get(data_list[1]) 
                    if response_str == None:
                        response = f"$-1\r\n".encode()
                    else:
                        response = format_response(response_str)
                case "CONFIG":
                    cfg_cmd = data_list[1].upper()
                    parameter =  data_list[2]
                    if cfg_cmd == "GET":
                        res: str = config[parameter]
                        # response = f"*2\r\n${len(parameter)}\r\n{parameter}\r\n${len(res)}\r\n{res}\r\n".encode()
                        response = format_array_response([parameter, res])
                    print(response)
                case "KEYS":
                    keys = get_keys(data_list[1])
                    response = format_array_response(keys)
                    print(response)

                case _:
                    response = format_response(data_list[-1])

        conn.send(response)
    conn.close()

def accept_connectins(server_socket: socket.socket):
    conn, addr = server_socket.accept()
    print(f"Connected to: {addr}")
    _thread.start_new_thread(client_handler, (conn, addr))


def main():
    parse_cli_args()

    load_rdb_file(config["dir"], config["dbfilename"])

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