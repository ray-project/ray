import aioredis
import argparse
import asyncio
import binascii
import collections
import datetime
import json
import numpy as np
import os
import redis
import sys
import time
import websockets

parser = argparse.ArgumentParser(description="parse information for the web ui")
parser.add_argument("--redis-address", required=True, type=str, help="the address to use for redis")

loop = asyncio.get_event_loop()

IDENTIFIER_LENGTH = 20

# This prefix must match the value defined in ray_redis_module.cc.
DB_CLIENT_PREFIX = b"CL:"

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

async def hgetall_as_dict(redis_conn, key):
  fields = await redis_conn.execute("hgetall", key)
  return {fields[2 * i]: fields[2 * i + 1] for i in range(len(fields) // 2)}


# Cache information about the local schedulers.
local_schedulers = {}
errors = []

def duration_to_string(duration):
  """Format a duration in seconds as a string.

  Args:
    duration (float): The duration in seconds.

  Return:
    A more human-readable version of the string (for example, "3.5 hours" or
      "93 milliseconds").
  """
  if duration > 3600 * 24:
    duration_str = "{0:0.1f} days".format(duration / (3600 * 24))
  elif duration > 3600:
    duration_str = "{0:0.1f} hours".format(duration / 3600)
  elif duration > 60:
    duration_str = "{0:0.1f} minutes".format(duration / 60)
  elif duration > 1:
    duration_str = "{0:0.1f} seconds".format(duration)
  elif duration > 0.001:
    duration_str = "{0:0.1f} milliseconds".format(duration * 1000)
  else:
    duration_str = "{} microseconds".format(int(duration * 1000000))
  return duration_str

async def handle_get_statistics(websocket, redis_conn):
  cluster_start_time = float(await redis_conn.execute("get", "redis_start_time"))
  start_date = timestamp_to_date_string(cluster_start_time)

  uptime = duration_to_string(time.time() - cluster_start_time)

  client_keys = await redis_conn.execute("keys", "CL:*")
  clients = []
  for client_key in client_keys:
    client_fields = await hgetall_as_dict(redis_conn, client_key)
    clients.append(client_fields)
  ip_addresses = list(set([client[b"node_ip_address"].decode("ascii") for client in clients if client[b"client_type"] == b"local_scheduler"]))
  num_nodes = len(ip_addresses)
  reply = {"uptime": uptime,
           "start_date": start_date,
           "nodes": num_nodes,
           "addresses": ip_addresses}
  await websocket.send(json.dumps(reply))

async def handle_get_drivers(websocket, redis_conn):
  keys = await redis_conn.execute("keys", "Drivers:*")
  drivers = []
  for key in keys:
    driver_fields = await hgetall_as_dict(redis_conn, key)
    driver_info = {"node ip address": driver_fields[b"node_ip_address"].decode("ascii"),
                   "name": driver_fields[b"name"].decode("ascii")}

    driver_info["start time"] = timestamp_to_date_string(float(driver_fields[b"start_time"]))

    if b"end_time" in driver_fields:
      duration = float(driver_fields[b"end_time"]) - float(driver_fields[b"start_time"])
    else:
      duration = time.time() - float(driver_fields[b"start_time"])
    driver_info["duration"] = duration_to_string(duration)

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

async def listen_for_errors(redis_ip_address, redis_port):
  pubsub_conn = await aioredis.create_connection((redis_ip_address, redis_port), loop=loop)
  data_conn = await aioredis.create_connection((redis_ip_address, redis_port), loop=loop)

  error_pattern = "__keyspace@0__:ErrorKeys"
  psub = await pubsub_conn.execute_pubsub("psubscribe", error_pattern)
  channel = pubsub_conn.pubsub_patterns[error_pattern]
  print("Listening for error messages...")
  index = 0
  while (await channel.wait_message()):
    msg = await channel.get()
    info = await data_conn.execute("lrange", "ErrorKeys", index, -1)

    for error_key in info:
      worker, task = key_to_hex_identifiers(error_key)
      # TODO(richard): Filter out workers so that only relevant task errors are
      # necessary.
      result = await data_conn.execute("hget", error_key, "message")
      result = result.decode("ascii")
      # TODO(richard): Maybe also get rid of the coloring.
      errors.append({"driver_id": worker,
                     "task_id": task,
                     "error": result})
      index += 1

async def handle_get_errors(websocket):
  """Send error messages to the frontend."""
  await websocket.send(json.dumps(errors))

node_info = collections.OrderedDict()
worker_info = collections.OrderedDict()

async def handle_get_recent_tasks(websocket, redis_conn, num_tasks):
  # First update the cache of worker information.
  worker_keys = await redis_conn.execute("keys", "Workers:*")
  for key in worker_keys:
    worker_id = hex_identifier(key[len("Workers:"):])
    if worker_id not in worker_info:
      worker_info[worker_id] = await hgetall_as_dict(redis_conn, key)
      node_ip_address = worker_info[worker_id][b"node_ip_address"].decode("ascii")
      if node_ip_address not in node_info:
        node_info[node_ip_address] = {"workers": []}
      node_info[node_ip_address]["workers"].append(worker_id)

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

    worker_ids = list(worker_info.keys())
    node_ip_addresses = list(node_info.keys())

    num_tasks = 0
    task_data = [{"task_data": [],
                  "num_workers": len(node_info[node_ip_address]["workers"])} for node_ip_address in node_ip_addresses]
    for i in range(len(keys)):
      worker_id, task_id = key_to_hex_identifiers(keys[i])
      data = contents[i]
      if worker_id not in worker_ids:
        # This case should be extremely rare.
        raise Exception("A worker ID was not present in the list of worker IDs.")
      node_ip_address = worker_info[worker_id][b"node_ip_address"].decode("ascii")
      worker_index = node_info[node_ip_address]["workers"].index(worker_id)
      node_index = node_ip_addresses.index(node_ip_address)

      task_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task"]
      if task_times[1] <= time_cutoff:
        continue

      task_get_arguments_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:get_arguments"]
      task_execute_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:execute"]
      task_store_outputs_times = [timestamp for (timestamp, task, kind, info) in data if task == "ray:task:store_outputs"]
      task_data[node_index]["task_data"].append(
          {"task": task_times,
           "get_arguments": task_get_arguments_times,
           "execute": task_execute_times,
           "store_outputs": task_store_outputs_times,
           "worker_index": worker_index,
           "node_ip_address": node_ip_address,
           "task_formatted_time": duration_to_string(task_times[1] - task_times[0]),
           "get_arguments_formatted_time": duration_to_string(task_get_arguments_times[1] - task_get_arguments_times[0]),
           "execute_formatted_time": duration_to_string(task_execute_times[1] - task_execute_times[0]),
           "store_outputs_formatted_time": duration_to_string(task_store_outputs_times[1] - task_store_outputs_times[0])})
      num_tasks += 1
    reply = {"min_time": min_time,
             "max_time": max_time,
             "num_tasks": num_tasks,
             "task_data": task_data}
    await websocket.send(json.dumps(reply))

async def send_heartbeat_payload(websocket):
  """Send heartbeat updates to the frontend every half second."""
  while True:
    reply = []
    for local_scheduler_id, local_scheduler in local_schedulers.items():
      current_time = time.time()
      local_scheduler_info = {"local scheduler ID": local_scheduler_id,
                              "time since heartbeat": duration_to_string(current_time - local_scheduler["last_heartbeat"]),
                              "time since heartbeat numeric": str(current_time - local_scheduler["last_heartbeat"]),
                              "node ip address": local_scheduler["node_ip_address"]}
      reply.append(local_scheduler_info)
    # Send the payload to the frontend.
    await websocket.send(json.dumps(reply))
    # Wait for a little while so as not to overwhelm the frontend.
    await asyncio.sleep(0.5)

async def send_heartbeats(websocket, redis_conn):
  # First update the local scheduler info locally.
  client_keys = await redis_conn.execute("keys", "CL:*")
  clients = []
  for client_key in client_keys:
    client_fields = await hgetall_as_dict(redis_conn, client_key)
    if client_fields[b"client_type"] == b"local_scheduler":
      local_scheduler_id = hex_identifier(client_fields[b"ray_client_id"])
      local_schedulers[local_scheduler_id] = {"node_ip_address": client_fields[b"node_ip_address"].decode("ascii"),
                                              "local_scheduler_socket_name": client_fields[b"local_scheduler_socket_name"].decode("ascii"),
                                              "aux_address": client_fields[b"aux_address"].decode("ascii"),
                                              "last_heartbeat": -1 * np.inf}

  # Subscribe to local scheduler heartbeats.
  await redis_conn.execute_pubsub("subscribe", "local_schedulers")

  # Start a method in the background to periodically update the frontend.
  asyncio.ensure_future(send_heartbeat_payload(websocket))

  while True:
    msg = await redis_conn.pubsub_channels["local_schedulers"].get()
    local_scheduler_id_bytes = msg[:IDENTIFIER_LENGTH]
    local_scheduler_id = hex_identifier(local_scheduler_id_bytes)
    if local_scheduler_id not in local_schedulers:
      # A new local scheduler has joined the cluster. Ignore it. This won't be
      # displayed in the UI until the page is refreshed.
      continue
    local_schedulers[local_scheduler_id]["last_heartbeat"] = time.time()

async def cache_data_from_redis(redis_ip_address, redis_port):
  """Open up ports to listen for new updates from Redis."""
  # TODO(richard): A lot of code needs to be ported in order to open new
  # websockets.

  asyncio.ensure_future(listen_for_errors(redis_ip_address, redis_port))

async def serve_requests(websocket, path):
  # We loop infinitely because otherwise the websocket will be closed.
  # TODO(rkn): Maybe we should open a new web sockets for every request instead
  # of looping here.
  redis_conn = await aioredis.create_connection((redis_ip_address, redis_port), loop=loop)
  while True:
    command = json.loads(await websocket.recv())
    print("received command {}".format(command))

    if command["command"] == "get-statistics":
      await handle_get_statistics(websocket, redis_conn)
    elif command["command"] == "get-drivers":
      await handle_get_drivers(websocket, redis_conn)
    elif command["command"] == "get-recent-tasks":
      await handle_get_recent_tasks(websocket, redis_conn, command["num"])
    elif command["command"] == "get-errors":
      await handle_get_errors(websocket)
    elif command["command"] == "get-heartbeats":
      await send_heartbeats(websocket, redis_conn)

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
      tasks = collections.defaultdict(list)
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

  # The port here must match the value used by the frontend to connect over
  # websockets. TODO(richard): Automatically increment the port if it is already
  # taken.
  port = 8888

  loop.run_until_complete(cache_data_from_redis(redis_ip_address, redis_port))

  start_server = websockets.serve(serve_requests, "localhost", port)
  loop.run_until_complete(start_server)
  loop.run_forever()
