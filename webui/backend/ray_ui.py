import aioredis
import argparse
import asyncio
import binascii
from collections import defaultdict
import datetime
import json
import numpy as np
import redis
import time
import websockets

parser = argparse.ArgumentParser(description="parse information for the web ui")
parser.add_argument("--port", type=int, help="port to use for the web ui")
parser.add_argument("--redis-address", required=True, type=str, help="the address to use for redis")

loop = asyncio.get_event_loop()

IDENTIFIER_LENGTH = 20

def hex_identifier(identifier):
  return binascii.hexlify(identifier).decode()

def identifier(hex_identifier):
  return binascii.unhexlify(hex_identifier)

def key_to_hex_identifier(key):
  return hex_identifier(key[(key.index(b":") + 1):(key.index(b":") + IDENTIFIER_LENGTH + 1)])

def timestamp_to_date_string(timestamp):
  """Convert a time stamp returned by time.time() to a formatted string."""
  return datetime.datetime.fromtimestamp(timestamp).strftime("%Y/%m/%d %H:%M:%S")

def key_to_hex_identifiers(key):
  # Extract worker_id and task_id from key of the form prefix:worker_id:task_id.
  offset = key.index(b":") + 1
  worker_id = hex_identifier(key[offset:(offset + IDENTIFIER_LENGTH)])
  offset += IDENTIFIER_LENGTH + 1
  task_id = hex_identifier(key[offset:(offset + IDENTIFIER_LENGTH)])
  return worker_id, task_id

worker_ids = []

async def handle_get_statistics(websocket, redis_conn):
  cluster_start_time = float(await redis_conn.execute("get", "redis_start_time"))
  start_date = timestamp_to_date_string(cluster_start_time)

  uptime = time.time() - cluster_start_time
  if uptime > 3600 * 24:
    uptime_str = "{0:0.1f} days".format(uptime / (3600 * 24))
  elif uptime > 3600:
    uptime_str = "{0:0.1f} hours".format(uptime / 3600)
  elif uptime > 60:
    uptime_str = "{0:0.1f} minutes".format(uptime / 60)
  else:
    uptime_str = "{0:0.1f} seconds".format(uptime)

  client_keys = await redis_conn.execute("keys", "CL:*")
  clients = []
  for client_key in client_keys:
    client_fields = await redis_conn.execute("hgetall", client_key)
    client_fields = {client_fields[2 * i]: client_fields[2 * i + 1] for i in range(len(client_fields) // 2)}
    clients.append(client_fields)
  ip_addresses = list(set([client[b"node_ip_address"].decode("ascii") for client in clients if client[b"client_type"] == b"photon"]))
  num_nodes = len(ip_addresses)
  reply = {"uptime": uptime_str,
           "start_date": start_date,
           "nodes": num_nodes,
           "addresses": ip_addresses}
  await websocket.send(json.dumps(reply))

async def handle_get_drivers(websocket, redis_conn):
  keys = await redis_conn.execute("keys", "Drivers:*")
  drivers = []
  for key in keys:
    driver_fields = await redis_conn.execute("hgetall", key)
    driver_fields = {driver_fields[2 * i]: driver_fields[2 * i + 1] for i in range(len(driver_fields) // 2)}
    driver_info = {"node ip address": driver_fields[b"node_ip_address"].decode("ascii"),
                   "name": driver_fields[b"name"].decode("ascii")}

    driver_info["start time"] = timestamp_to_date_string(float(driver_fields[b"start_time"]))

    if b"end_time" in driver_fields:
      duration = float(driver_fields[b"end_time"]) - float(driver_fields[b"start_time"])
    else:
      duration = time.time() - float(driver_fields[b"start_time"])
    if duration > 3600 * 24:
      driver_info["duration"] = "{0:0.1f} days".format(duration / (3600 * 24))
    elif duration > 3600:
      driver_info["duration"] = "{0:0.1f} hours".format(duration / 3600)
    elif duration > 60:
      driver_info["duration"] = "{0:0.1f} minutes".format(duration / 60)
    elif duration > 1:
      driver_info["duration"] = "{0:0.1f} seconds".format(duration)
    else:
      driver_info["duration"] = "{} milliseconds".format(int(duration * 1000))

    if b"exception" in driver_fields:
      driver_info["status"] = "FAILED"
    elif b"end_time" not in driver_fields:
      driver_info["status"] = "IN PROGRESS"
    else:
      driver_info["status"] = "SUCCESS"

    if b"exception" in driver_fields:
      driver_info["exception"] = driver_fields[b"exception"].decode("ascii")

    drivers.append(driver_info)
  # Sort the drivers by their start times.
  reply = sorted(drivers, key=(lambda driver: driver["start time"]))[::-1]
  await websocket.send(json.dumps(reply))

async def handle_get_recent_tasks(websocket, redis_conn, num_tasks):
  keys = await redis_conn.execute("keys", "event_log:*")
  if len(keys) == 0:
    # There are no tasks, so send a message to the client saying so.
    await websocket.send(json.dumps({"num_tasks": 0}))
  else:
    timestamps = []
    contents = []
    for key in keys:
      content = await redis_conn.execute("lrange", key, "0", "-1")
      contents.append(json.loads(content[0].decode()))
      timestamps += [timestamp for (timestamp, task, kind, info) in contents[-1] if task == "ray:task"]

    timestamps.sort()
    time_cutoff = timestamps[(-2 * num_tasks):][0]

    max_time = timestamps[-1]
    min_time = time_cutoff - (max_time - time_cutoff) * 0.1
    max_time = max_time + (max_time - time_cutoff) * 0.1

    task_data = []
    for i in range(len(keys)):
      worker_id, task_id = key_to_hex_identifiers(keys[i])
      data = contents[i]
      if worker_id not in worker_ids:
        worker_ids.append(worker_id)
      worker_index = worker_ids.index(worker_id)

      task_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task"]
      if task_times[1] <= time_cutoff:
        continue

      task_get_arguments_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:get_arguments"]
      task_execute_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:execute"]
      task_store_outputs_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:store_outputs"]
      task_data.append({"task": task_times,
                        "get_arguments": task_get_arguments_times,
                        "execute": task_execute_times,
                        "store_outputs": task_store_outputs_times,
                        "worker_index": worker_index})
    reply = {"min_time": min_time,
             "max_time": max_time,
             "num_tasks": len(task_data),
             "task_data": task_data}
    await websocket.send(json.dumps(reply))

async def serve_requests(websocket, path):
  redis_conn = await aioredis.create_connection((redis_ip_address, redis_port), loop=loop)

  # We loop infinitely because otherwise the websocket will be closed.
  # TODO(rkn): Maybe we should open a new web sockets for every request instead
  # of looping here.
  while True:
    command = json.loads(await websocket.recv())
    print("received command {}".format(command))

    if command["command"] == "get-statistics":
      await handle_get_statistics(websocket, redis_conn)
    elif command["command"] == "get-drivers":
      await handle_get_drivers(websocket, redis_conn)
    elif command["command"] == "get-recent-tasks":
      await handle_get_recent_tasks(websocket, redis_conn, command["num"])

    if command["command"] == "get-workers":
      result = []
      workers = await redis_conn.execute("keys", "WorkerInfo:*")
      for key in workers:
        content = await redis_conn.execute("hgetall", key)
        worker_id = key_to_hex_identifier(key)
        result.append({"worker": worker_id, "export_counter": int(content[1])})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-clients":
      result = []
      clients = await redis_conn.execute("keys", "CL:*")
      for key in clients:
        content = await redis_conn.execute("hgetall", key)
        result.append({"client": hex_identifier(content[1]),
                       "node_ip_address": content[3].decode(),
                       "client_type": content[5].decode()})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-objects":
      result = []
      objects = await redis_conn.execute("keys", "OI:*")
      for key in objects:
        content = await redis_conn.execute("hgetall", key)
        result.append({"object_id": hex_identifier(content[1]),
                       "hash": hex_identifier(content[3]),
                       "data_size": content[5].decode()})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-object-info":
      # TODO(pcm): Get the object here (have to connect to ray) and ship content
      # and type back to webclient. One challenge here is that the naive
      # implementation will block the web ui backend, which is not ok if it is
      # serving multiple users.
      await websocket.send(json.dumps({"object_id": "none"}))
    elif command["command"] == "get-tasks":
      result = []
      tasks = await redis_conn.execute("keys", "TT:*")
      for key in tasks:
        content = await redis_conn.execute("hgetall", key)
        result.append({"task_id": key_to_hex_identifier(key),
                       "state": int(content[1]),
                       "node_id": hex_identifier(content[3])})
      await websocket.send(json.dumps(result))
    elif command["command"] == "get-timeline":
      tasks = defaultdict(list)
      for key in await redis_conn.execute("keys", "event_log:*"):
        worker_id, task_id = key_to_hex_identifiers(key)
        content = await redis_conn.execute("lrange", key, "0", "-1")
        data = json.loads(content[0].decode())
        begin_and_end_time = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task"]
        tasks[worker_id].append({"task_id": task_id,
                                 "start_task": min(begin_and_end_time),
                                 "end_task": max(begin_and_end_time)})
      await websocket.send(json.dumps(tasks))
    elif command["command"] == "get-events":
      result = []
      for key in await redis_conn.execute("keys", "event_log:*"):
        worker_id, task_id = key_to_hex_identifiers(key)
        answer = await redis_conn.execute("lrange", key, "0", "-1")
        assert len(answer) == 1
        events = json.loads(answer[0].decode())
        result.extend([{"worker_id": worker_id,
                        "task_id": task_id,
                        "time": event[0],
                        "type": event[1]} for event in events])
      await websocket.send(json.dumps(result))

if __name__ == "__main__":
  args = parser.parse_args()
  redis_address = args.redis_address.split(":")
  redis_ip_address, redis_port = redis_address[0], int(redis_address[1])

  start_server = websockets.serve(serve_requests, "localhost", args.port)

  loop.run_until_complete(start_server)
  loop.run_forever()
