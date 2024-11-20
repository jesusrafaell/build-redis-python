import fnmatch
import os
import socket
import _thread
import struct
import sys
import time


class Redis:
    dir: str
    dbfilename: str
    db = {}

    def __init__(self, dir, dbfilename):
        self.dir = dir
        self.dbfilename = dbfilename

    def _encode(self, msg: str):
        res = f"+{msg}\r\n"
        return res.encode()
    
    def ping(self):
        return self._encode("PONG")

    def echo(self, value):
        return self._encode(value)
    
    def set(self, key: str, value: str, px=None):
        expiration = None
        if px: 
            expiration = time.time() + px / 1000  

        self.db[key] = (value, expiration)
        print(f"Key '{key}' set with value '{value}' and expiration {expiration}")

        return self._encode("OK")

    def get(self, key: str) -> str:
        print(f"-------------{key}")
        if key not in self.db:
            return b"$-1\r\n"

        value, expiration = self.db[key]
        if expiration and time.time() > expiration: 
            del self.db[key]
            print(f"Key '{key}' has expired and was deleted.")
            return b"$-1\r\n"
        
        if value is None:
            return b"$-1\r\n"
        return self._encode(value)

    def config(self, method: str):
        if "GET" in method.upper():
            key = "dir"
            value = self.dir
            return self.format_array_response([key, value])
        else:
            return b"*2\r\n$-1\r\n$-1\r\n"

    def key(self, key):
        keys = self.filter_keys(key)
        response = self.format_array_response(keys)
        return response

    def filter_keys(self, pattern: str) -> list[str]:
        keys = fnmatch.filter(self.db.keys(), pattern)
        return keys


    def format_array_response(self, values: list[str]) -> bytes:
        response = f"*{len(values)}\r\n"
        for value in values:
            response += f"${len(value)}\r\n{value}\r\n"
        return response.encode()

    def format_response(self, value: str) -> bytes:
        response = f"${len(value)}\r\n{value}\r\n"
        return self._encode(response)

    def delete_expired_keys(self):
        while True:
            current_time = time.time()
            keys_to_delete = [key for key, (_, exp) in self.db.items() if exp and current_time > exp]
            for key in keys_to_delete:
                del self.db[key]
                print(f"Key '{key}' expired and was removed.")
            time.sleep(1)  
    
    def commands(self, command, args):
        res = ""
        match command.upper():
            case "PING":
                res =  self.ping()
            case "ECHO":
                res = self.echo(args[0])
            case "SET":
                px = int(args[3]) if len(args) >= 4 and args[2].upper() == "PX" else None
                res = self.set(args[0], args[1], px)
            case "GET":
                res = self.get(args[0])
            case "CONFIG":
                res = self.config(args[0])
            case "KEYS":
                res = self.key(args[0] if len(args) > 0 else "*")
                print(f"_____________________{res}")
            case _:
                res = self.echo(args[0])
        return res

    def remove_bytes_caracteres(self, string: str) -> str:
        if string.startswith("x"):
            return string[3:]
        elif string.startswith("t"):
            return string[1:]


    def parse_redis_file_format(self, file_format: str) -> tuple[str, str]:
        splited_parts = file_format.split("\\")
        resizedb_index = splited_parts.index("xfb")

        key_index = resizedb_index + 4
        value_index = key_index + 1

        key_bytes = splited_parts[key_index]
        value_bytes = splited_parts[value_index]

        key = self.remove_bytes_caracteres(key_bytes)
        value = self.remove_bytes_caracteres(value_bytes)

        # print(key, value)


        return key, value


    def load_file(self):
        rdb_file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(rdb_file_path):
            with open(rdb_file_path, "rb") as rdb_file:
                rdb_content = str(rdb_file.read())
                print("rbd content", rdb_content)
                if rdb_content:
                    key, value = self.parse_redis_file_format(rdb_content)
                    #save
                    self.set(key, value)
        # If RDB file doesn't exist or no args provided, return
        return "*0\r\n".encode()



def client_handler(conn: socket.socket, addr, redis: Redis): 
    while True:
        data = conn.recv(1024)
        if not data: 
            print(f"Connection closed by {addr}")
            break

        decode_msg = data.decode().strip().split()
        args = [d for d in decode_msg if not d.startswith(("*", "$"))]

        print(f"data: {args}")

        command = args[0]
        args = args[1:]

        response =  redis.commands(command, args)

        conn.send(response)
    conn.close()

def accept_connectins(server_socket: socket.socket, redis: Redis):
    conn, addr = server_socket.accept()
    print(f"Connected to: {addr}")
    _thread.start_new_thread(client_handler, (conn, addr, redis))

def parse_cli_args():
    dir, dbfilename = "", ""
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "--dir":
            dir = sys.argv[i + 1]
        elif sys.argv[i] == "--dbfilename":
            dbfilename = sys.argv[i + 1]
    return dir, dbfilename

def main():
    dir, dbfilename  = parse_cli_args()

    redis = Redis(dir, dbfilename)

    redis.load_file()

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    print(f'Server is listing')
    try:
        while True:
            accept_connectins(server_socket, redis)
    except KeyboardInterrupt:
        print("Caught keyboard interrupt, exiting")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()