from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import pickle
import redis

import ray
from ray.utils import (decode, binary_to_object_id, binary_to_hex,
                       hex_to_binary)

# Import flatbuffer bindings.
from ray.core.generated.TaskInfo import TaskInfo
from ray.core.generated.TaskReply import TaskReply
from ray.core.generated.ResultTableReply import ResultTableReply

# These prefixes must be kept up-to-date with the definitions in
# ray_redis_module.cc.
DB_CLIENT_PREFIX = "CL:"
OBJECT_INFO_PREFIX = "OI:"
OBJECT_LOCATION_PREFIX = "OL:"
OBJECT_SUBSCRIBE_PREFIX = "OS:"
TASK_PREFIX = "TT:"
FUNCTION_PREFIX = "RemoteFunction:"
OBJECT_CHANNEL_PREFIX = "OC:"

# This mapping from integer to task state string must be kept up-to-date with
# the scheduling_state enum in task.h.
TASK_STATUS_WAITING = 1
TASK_STATUS_SCHEDULED = 2
TASK_STATUS_QUEUED = 4
TASK_STATUS_RUNNING = 8
TASK_STATUS_DONE = 16
TASK_STATUS_LOST = 32
TASK_STATUS_RECONSTRUCTING = 64
TASK_STATUS_MAPPING = {
    TASK_STATUS_WAITING: "WAITING",
    TASK_STATUS_SCHEDULED: "SCHEDULED",
    TASK_STATUS_QUEUED: "QUEUED",
    TASK_STATUS_RUNNING: "RUNNING",
    TASK_STATUS_DONE: "DONE",
    TASK_STATUS_LOST: "LOST",
    TASK_STATUS_RECONSTRUCTING: "RECONSTRUCTING",
}


class GlobalState(object):
  """A class used to interface with the Ray control state.

  Attributes:
    redis_client: The redis client used to query the redis server.
  """
  def __init__(self):
    """Create a GlobalState object."""
    self.redis_client = None

  def _check_connected(self):
    """Check that the object has been initialized before it is used.

    Raises:
      Exception: An exception is raised if ray.init() has not been called yet.
    """
    if self.redis_client is None:
      raise Exception("The ray.global_state API cannot be used before "
                      "ray.init has been called.")

  def _initialize_global_state(self, redis_ip_address, redis_port):
    """Initialize the GlobalState object by connecting to Redis.

    Args:
      redis_ip_address: The IP address of the node that the Redis server lives
        on.
      redis_port: The port that the Redis server is listening on.
    """
    self.redis_client = redis.StrictRedis(host=redis_ip_address,
                                          port=redis_port)
    self.redis_clients = []
    num_redis_shards = self.redis_client.get("NumRedisShards")
    if num_redis_shards is None:
      raise Exception("No entry found for NumRedisShards")
    num_redis_shards = int(num_redis_shards)
    if (num_redis_shards < 1):
      raise Exception("Expected at least one Redis shard, found "
                      "{}.".format(num_redis_shards))

    ip_address_ports = self.redis_client.lrange("RedisShards", start=0, end=-1)
    if len(ip_address_ports) != num_redis_shards:
      raise Exception("Expected {} Redis shard addresses, found "
                      "{}".format(num_redis_shards, len(ip_address_ports)))

    for ip_address_port in ip_address_ports:
      shard_address, shard_port = ip_address_port.split(b":")
      self.redis_clients.append(redis.StrictRedis(host=shard_address,
                                                  port=shard_port))

  def _execute_command(self, key, *args):
    """Execute a Redis command on the appropriate Redis shard based on key.

    Args:
      key: The object ID or the task ID that the query is about.
      args: The command to run.

    Returns:
      The value returned by the Redis command.
    """
    client = self.redis_clients[key.redis_shard_hash() %
                                len(self.redis_clients)]
    return client.execute_command(*args)

  def _keys(self, pattern):
    """Execute the KEYS command on all Redis shards.

    Args:
      pattern: The KEYS pattern to query.

    Returns:
      The concatenated list of results from all shards.
    """
    result = []
    for client in self.redis_clients:
      result.extend(client.keys(pattern))
    return result

  def _object_table(self, object_id):
    """Fetch and parse the object table information for a single object ID.

    Args:
      object_id_binary: A string of bytes with the object ID to get information
        about.

    Returns:
      A dictionary with information about the object ID in question.
    """
    # Return information about a single object ID.
    object_locations = self._execute_command(object_id,
                                             "RAY.OBJECT_TABLE_LOOKUP",
                                             object_id.id())
    if object_locations is not None:
      manager_ids = [binary_to_hex(manager_id)
                     for manager_id in object_locations]
    else:
      manager_ids = None

    result_table_response = self._execute_command(object_id,
                                                  "RAY.RESULT_TABLE_LOOKUP",
                                                  object_id.id())
    result_table_message = ResultTableReply.GetRootAsResultTableReply(
        result_table_response, 0)

    result = {"ManagerIDs": manager_ids,
              "TaskID": binary_to_hex(result_table_message.TaskId()),
              "IsPut": bool(result_table_message.IsPut()),
              "DataSize": result_table_message.DataSize(),
              "Hash": binary_to_hex(result_table_message.Hash())}

    return result

  def object_table(self, object_id=None):
    """Fetch and parse the object table information for one or more object IDs.

    Args:
      object_id: An object ID to fetch information about. If this is None, then
        the entire object table is fetched.


    Returns:
      Information from the object table.
    """
    self._check_connected()
    if object_id is not None:
      # Return information about a single object ID.
      return self._object_table(object_id)
    else:
      # Return the entire object table.
      object_info_keys = self._keys(OBJECT_INFO_PREFIX + "*")
      object_location_keys = self._keys(OBJECT_LOCATION_PREFIX + "*")
      object_ids_binary = set(
          [key[len(OBJECT_INFO_PREFIX):] for key in object_info_keys] +
          [key[len(OBJECT_LOCATION_PREFIX):] for key in object_location_keys])
      results = {}
      for object_id_binary in object_ids_binary:
        results[binary_to_object_id(object_id_binary)] = self._object_table(
            binary_to_object_id(object_id_binary))
      return results

  def _task_table(self, task_id):
    """Fetch and parse the task table information for a single object task ID.

    Args:
      task_id_binary: A string of bytes with the task ID to get information
        about.

    Returns:
      A dictionary with information about the task ID in question.
      TASK_STATUS_MAPPING should be used to parse the "State" field into a
      human-readable string.
    """
    task_table_response = self._execute_command(task_id,
                                                "RAY.TASK_TABLE_GET",
                                                task_id.id())
    if task_table_response is None:
      raise Exception("There is no entry for task ID {} in the task table."
                      .format(binary_to_hex(task_id.id())))
    task_table_message = TaskReply.GetRootAsTaskReply(task_table_response, 0)
    task_spec = task_table_message.TaskSpec()
    task_spec_message = TaskInfo.GetRootAsTaskInfo(task_spec, 0)
    args = []
    for i in range(task_spec_message.ArgsLength()):
      arg = task_spec_message.Args(i)
      if len(arg.ObjectId()) != 0:
        args.append(binary_to_object_id(arg.ObjectId()))
      else:
        args.append(pickle.loads(arg.Data()))
    assert task_spec_message.RequiredResourcesLength() == 2
    required_resources = {"CPUs": task_spec_message.RequiredResources(0),
                          "GPUs": task_spec_message.RequiredResources(1)}
    task_spec_info = {
        "DriverID": binary_to_hex(task_spec_message.DriverId()),
        "TaskID": binary_to_hex(task_spec_message.TaskId()),
        "ParentTaskID": binary_to_hex(task_spec_message.ParentTaskId()),
        "ParentCounter": task_spec_message.ParentCounter(),
        "ActorID": binary_to_hex(task_spec_message.ActorId()),
        "ActorCounter": task_spec_message.ActorCounter(),
        "FunctionID": binary_to_hex(task_spec_message.FunctionId()),
        "Args": args,
        "ReturnObjectIDs": [binary_to_object_id(task_spec_message.Returns(i))
                            for i in range(task_spec_message.ReturnsLength())],
        "RequiredResources": required_resources}

    return {"State": task_table_message.State(),
            "LocalSchedulerID": binary_to_hex(
                task_table_message.LocalSchedulerId()),
            "TaskSpec": task_spec_info}

  def task_table(self, task_id=None):
    """Fetch and parse the task table information for one or more task IDs.

    Args:
      task_id: A hex string of the task ID to fetch information about. If this
        is None, then the task object table is fetched.


    Returns:
      Information from the task table.
    """
    self._check_connected()
    if task_id is not None:
      task_id = ray.local_scheduler.ObjectID(hex_to_binary(task_id))
      return self._task_table(task_id)
    else:
      task_table_keys = self._keys(TASK_PREFIX + "*")
      results = {}
      for key in task_table_keys:
        task_id_binary = key[len(TASK_PREFIX):]
        results[binary_to_hex(task_id_binary)] = self._task_table(
            ray.local_scheduler.ObjectID(task_id_binary))
      return results

  def function_table(self, function_id=None):
    """Fetch and parse the function table.

    Returns:
      A dictionary that maps function IDs to information about the function.
    """
    self._check_connected()
    function_table_keys = self.redis_client.keys(FUNCTION_PREFIX + "*")
    results = {}
    for key in function_table_keys:
      info = self.redis_client.hgetall(key)
      function_info_parsed = {
          "DriverID": binary_to_hex(info[b"driver_id"]),
          "Module": decode(info[b"module"]),
          "Name": decode(info[b"name"])
      }
      results[binary_to_hex(info[b"function_id"])] = function_info_parsed
    return results

  def client_table(self):
    """Fetch and parse the Redis DB client table.

    Returns:
      Information about the Ray clients in the cluster.
    """
    self._check_connected()
    db_client_keys = self.redis_client.keys(DB_CLIENT_PREFIX + "*")
    node_info = dict()
    for key in db_client_keys:
      client_info = self.redis_client.hgetall(key)
      node_ip_address = decode(client_info[b"node_ip_address"])
      if node_ip_address not in node_info:
        node_info[node_ip_address] = []
      client_info_parsed = {
          "ClientType": decode(client_info[b"client_type"]),
          "Deleted": bool(int(decode(client_info[b"deleted"]))),
          "DBClientID": binary_to_hex(client_info[b"ray_client_id"])
      }
      if b"aux_address" in client_info:
        client_info_parsed["AuxAddress"] = decode(client_info[b"aux_address"])
      if b"num_cpus" in client_info:
        client_info_parsed["NumCPUs"] = float(decode(client_info[b"num_cpus"]))
      if b"num_gpus" in client_info:
        client_info_parsed["NumGPUs"] = float(decode(client_info[b"num_gpus"]))
      if b"local_scheduler_socket_name" in client_info:
        client_info_parsed["LocalSchedulerSocketName"] = decode(
            client_info[b"local_scheduler_socket_name"])
      node_info[node_ip_address].append(client_info_parsed)

    return node_info

  def log_files(self):
    """Fetch and return a dictionary of log file names to outputs.

    Returns:
      IP address to log file name to log file contents mappings.
    """
    relevant_files = self.redis_client.keys("LOGFILE*")

    ip_filename_file = dict()

    for filename in relevant_files:
      filename = filename.decode("ascii")
      filename_components = filename.split(":")
      ip_addr = filename_components[1]

      file = self.redis_client.lrange(filename, 0, -1)
      file_str = []
      for x in file:
        y = x.decode("ascii")
        file_str.append(y)

      if ip_addr not in ip_filename_file:
        ip_filename_file[ip_addr] = dict()

      ip_filename_file[ip_addr][filename] = file_str

    return ip_filename_file

  def task_profiles(self):
    """Fetch and return a list of task profiles.

    Returns:
      A tuple of two elements. The first element is a dictionary mapping the
        task ID of a task to a list of the profiling information for all of the
        executions of that task. The second element is a list of profiling
        information for tasks where the events have no task ID.
    """
    task_info = dict()
    event_names = self.redis_client.keys("event_log*")
    for i in range(len(event_names)):
      event_list = self.redis_client.lrange(event_names[i], 0, -1)
      for event in event_list:
        event_dict = json.loads(event.decode("utf-8"))
        task_id = ""
        for event in event_dict:
          if "task_id" in event[3]:
            task_id = event[3]["task_id"]
        task_info[task_id] = dict()
        for event in event_dict:
          if event[1] == "ray:get_task" and event[2] == 1:
            task_info[task_id]["get_task_start"] = event[0]
          if event[1] == "ray:get_task" and event[2] == 2:
            task_info[task_id]["get_task_end"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 1:
            task_info[task_id]["import_remote_start"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 2:
            task_info[task_id]["import_remote_end"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 1:
            task_info[task_id]["acquire_lock_start"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 2:
            task_info[task_id]["acquire_lock_end"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 1:
            task_info[task_id]["get_arguments_start"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 2:
            task_info[task_id]["get_arguments_end"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 1:
            task_info[task_id]["execute_start"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 2:
            task_info[task_id]["execute_end"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 1:
            task_info[task_id]["store_outputs_start"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 2:
            task_info[task_id]["store_outputs_end"] = event[0]
          if "worker_id" in event[3]:
            task_info[task_id]["worker_id"] = event[3]["worker_id"]
          if "function_name" in event[3]:
            task_info[task_id]["function_name"] = event[3]["function_name"]
    return task_info

  def dump_catapult_trace(self, path):
    task_info = self.task_profiles()
    workers = self.workers()
    tasks = self.task_table()
    start_time = None
    for info in task_info.values():
      task_start = min(self.get_times(info))
      if not start_time or task_start < start_time:
        start_time = task_start
    def micros(ts):
      return int(1e6 * (ts - start_time))
    with open(path, 'w') as f:
      f.write("[\n")
      for i, (task_id, info) in enumerate(task_info.items()):
        parent_info = task_info.get(tasks[task_id]["TaskSpec"]["ParentTaskID"])
        times = self.get_times(info)
        worker = workers[info["worker_id"]]
        if i > 0:
          f.write(",\n")
        if parent_info:
          parent_worker = workers[parent_info["worker_id"]]
          parent_times = self.get_times(parent_info)
          f.write(json.dumps({
            "cat": "submit_task",
            "pid": "Node " + str(parent_worker["node_ip_address"]),
            "tid": parent_worker["worker_index"],
            "ts": micros(min(parent_times)),
            "ph": "s",
            "name": "SubmitTask",
            "args": {},
            "id": str(i),
          }))
          f.write(",\n")
          f.write(json.dumps({
            "cat": "submit_task",
            "pid": "Node " + str(worker["node_ip_address"]),
            "tid": worker["worker_index"],
            "ts": micros(min(times)),
            "ph": "f",
            "name": "SubmitTask",
            "args": {},
            "id": str(i),
          }))
          f.write(",\n")
        f.write(json.dumps({
          "name": info["function_name"],
          "cat": "ray_task",
          "ph": "X",
          "ts": micros(min(times)),
          "dur": micros(max(times)) - micros(min(times)),
          "pid": "Node " + str(worker["node_ip_address"]),
          "tid": worker["worker_index"],
          "args": info
        }))
      f.write("]")
    task_info

  def get_times(self, data):
    all_times = []
    all_times.append(data["acquire_lock_start"])
    all_times.append(data["acquire_lock_end"])
    all_times.append(data["get_arguments_start"])
    all_times.append(data["get_arguments_end"])
    all_times.append(data["execute_start"])
    all_times.append(data["execute_end"])
    all_times.append(data["store_outputs_start"])
    all_times.append(data["store_outputs_end"])
    return all_times

  def actor_classes(self):
    actor_info = dict()
    actors = self.redis_client.keys("ActorClass*")
    actor_classes = dict()
    for actor in actors:
      actor_key_str = actor[len('Actor:'):]
      actor_info['ActorClass:{}'.format(binary_to_hex(actor))] = self.redis_client.hgetall(actor)
      actor_key = actor_info['ActorClass:{}'.format(binary_to_hex(actor))]
      actor_classes[binary_to_hex(actor)] = {
        "driver_id": binary_to_hex(actor_key[b"driver_id"]),
        "class": actor_key[b"class"],
        "class_name": actor_key[b"class_name"].decode("ascii"),
        "module": actor_key[b"module"].decode("ascii"),
        "actor_method_names": actor_key[b"actor_method_names"].decode("ascii"),
        }
    return actor_classes

  def workers(self):
    workers = self.redis_client.keys("Worker*")
    worker_info = dict()
    workers_data = dict()
    i = 0
    for worker in workers:
      worker_key = worker[len("Workers:"):]
      worker_info["Workers:{}".format(binary_to_hex(worker_key))] = self.redis_client.hgetall(worker)
      worker_dict = worker_info["Workers:{}".format(binary_to_hex(worker_key))]
      workers_data[binary_to_hex(worker)[16:]] = {
        "worker_index": i,
        "local_scheduler_socket": binary_to_hex(worker_dict[b"local_scheduler_socket"]),
        "node_ip_address": binary_to_hex(worker_dict[b"node_ip_address"]),
        "plasma_manager_socket": binary_to_hex(worker_dict[b"plasma_manager_socket"]),
        "plasma_store_socket": binary_to_hex(worker_dict[b"plasma_store_socket"]),
        "stderr_file": binary_to_hex(worker_dict[b"stderr_file"]),
        "stdout_file": binary_to_hex(worker_dict[b"stdout_file"])
      }
      i += 1
    return workers_data

