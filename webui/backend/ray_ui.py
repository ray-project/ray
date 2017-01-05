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

def get_hex_identifier(key):
  return binascii.hexlify(key[key.index(b':')+1:key.index(b':')+21]).decode()

async def hello(websocket, path):
  conn = await aioredis.create_connection((redis_host, redis_port), loop=loop)

  command = await websocket.recv()
  print("< {}".format(json.loads(command)))

  if command["command"] == "get-workers":
    workers = await conn.execute('keys', 'Workers')
    for key in workers:
      
      worker_id = binascii.hexlify(key[len(b'Workers'):len(b'Workers')+20]).decode()

  tasks = defaultdict(list)
  for key in await conn.execute('keys', 'event_log:*'):
    worker_id = binascii.hexlify(key[len(b'event_log:'):len(b'event_log:')+20]).decode()
    content = await conn.execute('lrange', key, "0", "-1")
    tasks[worker_id].extend([json.loads(entry.decode()) for entry in content])

  await websocket.send(json.dumps(tasks))

if __name__ == "__main__":
  args = parser.parse_args()
  redis_address = args.redis_address.split(":")
  redis_host, redis_port = redis_address[0], int(redis_address[1])

  start_server = websockets.serve(hello, 'localhost', args.port)

  loop.run_until_complete(start_server)
  loop.run_forever()
