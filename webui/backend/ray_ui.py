import argparse
import asyncio
import aioredis
import websockets
import json
import binascii
from collections import defaultdict

parser = argparse.ArgumentParser(description="parse information for the web ui")
parser.add_argument("--port", type=int, help="port to use for the web ui")
parser.add_argument("--redis-address", required=True, type=str, help="the address to use for redis")

loop = asyncio.get_event_loop()

redis_host = "127.0.0.1"
redis_port = 6379

IDENTIFIER_LENGTH = 20

def hex_identifier(identifier):
  return binascii.hexlify(identifier).decode()

def identifier(hex_identifier):
  return binascii.unhexlify(hex_identifier)

def key_to_hex_identifier(key):
  return hex_identifier(key[key.index(b":")+1:key.index(b":")+IDENTIFIER_LENGTH+1])

async def hello(websocket, path):
  conn = await aioredis.create_connection((redis_host, redis_port), loop=loop)

  while True:
    command = json.loads(await websocket.recv())
    print("received command {}".format(command))

    if command["command"] == "get-workers":
      result = []
      workers = await conn.execute("keys", "WorkerInfo:*")
      for key in workers:
        content = await conn.execute("hgetall", key)
        worker_id = key_to_hex_identifier(key)
        result.append({"worker": worker_id, "export_counter": int(content[1])})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-clients":
      result = []
      clients = await conn.execute("keys", "CL:*")
      for key in clients:
        content = await conn.execute("hgetall", key)
        result.append({"client": hex_identifier(content[1]), "node_ip_address": content[3].decode(), "client_type": content[5].decode()})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-objects":
      result = []
      objects = await conn.execute("keys", "OI:*")
      for key in objects:
        content = await conn.execute("hgetall", key)
        result.append({"object_id": hex_identifier(content[1]), "hash": hex_identifier(content[3]), "data_size": content[5].decode()})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-object-info":
      # TODO(pcm): get object here (have to connect to ray)
      # and ship content and type back to webclient; one challenge
      # here is that the naive implementation will block the
      # web ui backend, which is not ok if it is serving multiple
      # users
      await websocket.send(json.dumps({"object_id": "none"}))
    elif command["command"] == "get-tasks":
      result = []
      tasks = await conn.execute("keys", "TT:*")
      for key in tasks:
        content = await conn.execute("hgetall", key)
        result.append({"task_id": key_to_hex_identifier(key), "state": int(content[1]), "node_id": hex_identifier(content[3])})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-timeline":
      tasks = defaultdict(list)
      for key in await conn.execute("keys", "event_log:*"):
        worker_id = key_to_hex_identifier(key)
        content = await conn.execute("lrange", key, "0", "-1")
        tasks[worker_id].extend([json.loads(entry.decode()) for entry in content])
      await websocket.send(json.dumps(tasks))
    elif command["command"] == "get-events":
      result = []
      for key in await conn.execute("keys", "event_log:*"):
        result.append("hello")
      await websocket.send(json.dumps(result))

if __name__ == "__main__":
  args = parser.parse_args()
  redis_address = args.redis_address.split(":")
  redis_host, redis_port = redis_address[0], int(redis_address[1])

  start_server = websockets.serve(hello, 'localhost', args.port)

  loop.run_until_complete(start_server)
  loop.run_forever()
