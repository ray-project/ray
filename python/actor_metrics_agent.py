
import asyncio
from lib2to3.pgen2.token import ASYNC
import os
import socket
from threading import Thread
from time import sleep
import psutil
from http.server import BaseHTTPRequestHandler, HTTPServer
import time

ip = socket.gethostbyname(socket.gethostname())

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        global dumps

        self.path = ''
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(bytes("# HELP ray_worker_resource_data data\n", encoding='utf8'))
        self.wfile.write(bytes("# TYPE ray_worker_resource_data gauge\n", encoding='utf8'))
        
        for _, dump in dumps.items():
            dumped = dump_str(dump)
            self.wfile.write(bytes(dumped, encoding='utf8'))
            print(dumped)


def dump_str(dump):
    ip = dump["ip"]
    name = dump["name"]
    pid = dump["pid"]

    str = ""
    for key in ["cpu", "rss", "uss"]:
        value = float(dump[key])
        str = str + f'ray_worker_resource_data_{key}{{Actor="{name}",NodeAddress="{ip}",Pid="{pid}"}} {value}' + '\n'
    return str

async def gen_dump(process, name, pid, ip):
    try:
        dump = {}
        dump['name'] = name
        dump['pid'] = pid
        dump["cpu"] = process.cpu_percent(interval=None)
        dump["rss"] = int(process.memory_full_info().rss / 1024**2)
        dump["uss"] = int(process.memory_full_info().uss / 1024**2)
        dump["shared"] = int(process.memory_full_info().shared / 1024**2)
        dump["ip"] = ip

        return dump
    except psutil.NoSuchProcess:
        return None
        

async def main():
    global dumps
    actors = {}
    while True:
        try:
            with open("/tmp/ray/session_latest/logs/hackathon_dump.txt") as f:
                lines = f.readlines()
                lines = lines[14:-1]
                
                updated_actors = {}
                index = 0
                while index < len(lines):
                    name = lines[index].strip().split('.')[0]
                    pid = int(lines[index+1].strip())
                    res = lines[index+2].strip()
                    if pid not in actors:
                        process = psutil.Process(pid)
                        updated_actors[pid] = (name, res, process)
                    else:
                        updated_actors[pid] = actors[pid]
                    index = index + 3

                actors = updated_actors
            
            print(f'# actors {len(actors)}')

            updated_dumps = {}
            for pid, value in actors.items():
                name, res, process = value
                dump = await gen_dump(process, name, pid, ip)
                if dump:
                    updated_dumps[pid] = dump
            dumps = updated_dumps
        except psutil.NoSuchProcess as err:
            print(f"p dead {err}")
            
        await asyncio.sleep(5)


def ws():
    webServer = HTTPServer(("localhost", 1234), MyServer)
    try:
        print("Server starting.")
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass
    
    webServer.server_close()
    print("Server stopped.")

if __name__ == "__main__":
    
    thread = Thread(target=ws) 
    thread.start()

    asyncio.get_event_loop().run_until_complete(main())
    
    
        

    

