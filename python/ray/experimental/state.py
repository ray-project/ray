from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
from collections import defaultdict
import heapq
import json
import redis
import sys
import time

import ray
import ray.gcs_utils
import ray.ray_constants as ray_constants
from ray.utils import (decode, binary_to_object_id, binary_to_hex,
                       hex_to_binary)


class GlobalState(object):
    """A class used to interface with the Ray control state.

    # TODO(zongheng): In the future move this to use Ray's redis module in the
    # backend to cut down on # of request RPCs.

    Attributes:
        redis_client: The Redis client used to query the primary redis server.
        redis_clients: Redis clients for each of the Redis shards.
    """

    def __init__(self):
        """Create a GlobalState object."""
        # The redis server storing metadata, such as function table, client
        # table, log files, event logs, workers/actions info.
        self.redis_client = None
        # Clients for the redis shards, storing the object table & task table.
        self.redis_clients = None

    def _check_connected(self):
        """Check that the object has been initialized before it is used.

        Raises:
            Exception: An exception is raised if ray.init() has not been called
                yet.
        """
        if self.redis_client is None:
            raise Exception("The ray.global_state API cannot be used before "
                            "ray.init has been called.")

        if self.redis_clients is None:
            raise Exception("The ray.global_state API cannot be used before "
                            "ray.init has been called.")

    def _initialize_global_state(self,
                                 redis_ip_address,
                                 redis_port,
                                 redis_password=None,
                                 timeout=20):
        """Initialize the GlobalState object by connecting to Redis.

        It's possible that certain keys in Redis may not have been fully
        populated yet. In this case, we will retry this method until they have
        been populated or we exceed a timeout.

        Args:
            redis_ip_address: The IP address of the node that the Redis server
                lives on.
            redis_port: The port that the Redis server is listening on.
            redis_password: The password of the redis server.
        """
        self.redis_client = redis.StrictRedis(
            host=redis_ip_address, port=redis_port, password=redis_password)

        start_time = time.time()

        num_redis_shards = None
        ip_address_ports = []

        while time.time() - start_time < timeout:
            # Attempt to get the number of Redis shards.
            num_redis_shards = self.redis_client.get("NumRedisShards")
            if num_redis_shards is None:
                print("Waiting longer for NumRedisShards to be populated.")
                time.sleep(1)
                continue
            num_redis_shards = int(num_redis_shards)
            if (num_redis_shards < 1):
                raise Exception("Expected at least one Redis shard, found "
                                "{}.".format(num_redis_shards))

            # Attempt to get all of the Redis shards.
            ip_address_ports = self.redis_client.lrange(
                "RedisShards", start=0, end=-1)
            if len(ip_address_ports) != num_redis_shards:
                print("Waiting longer for RedisShards to be populated.")
                time.sleep(1)
                continue

            # If we got here then we successfully got all of the information.
            break

        # Check to see if we timed out.
        if time.time() - start_time >= timeout:
            raise Exception("Timed out while attempting to initialize the "
                            "global state. num_redis_shards = {}, "
                            "ip_address_ports = {}".format(
                                num_redis_shards, ip_address_ports))

        # Get the rest of the information.
        self.redis_clients = []
        for ip_address_port in ip_address_ports:
            shard_address, shard_port = ip_address_port.split(b":")
            self.redis_clients.append(
                redis.StrictRedis(
                    host=shard_address,
                    port=shard_port,
                    password=redis_password))

    def _execute_command(self, key, *args):
        """Execute a Redis command on the appropriate Redis shard based on key.

        Args:
            key: The object ID or the task ID that the query is about.
            args: The command to run.

        Returns:
            The value returned by the Redis command.
        """
        client = self.redis_clients[key.redis_shard_hash() % len(
            self.redis_clients)]
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
            result.extend(list(client.scan_iter(match=pattern)))
        return result

    def _object_table(self, object_id):
        """Fetch and parse the object table information for a single object ID.

        Args:
            object_id_binary: A string of bytes with the object ID to get
                information about.

        Returns:
            A dictionary with information about the object ID in question.
        """
        # Allow the argument to be either an ObjectID or a hex string.
        if not isinstance(object_id, ray.ObjectID):
            object_id = ray.ObjectID(hex_to_binary(object_id))

        # Return information about a single object ID.
        message = self._execute_command(object_id, "RAY.TABLE_LOOKUP",
                                        ray.gcs_utils.TablePrefix.OBJECT, "",
                                        object_id.id())
        gcs_entry = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            message, 0)

        assert gcs_entry.EntriesLength() > 0

        entry = ray.gcs_utils.ObjectTableData.GetRootAsObjectTableData(
            gcs_entry.Entries(0), 0)

        object_info = {
            "DataSize": entry.ObjectSize(),
            "Manager": entry.Manager(),
            "IsEviction": [entry.IsEviction()],
        }

        for i in range(1, gcs_entry.EntriesLength()):
            entry = ray.gcs_utils.ObjectTableData.GetRootAsObjectTableData(
                gcs_entry.Entries(i), 0)
            object_info["IsEviction"].append(entry.IsEviction())

        return object_info

    def object_table(self, object_id=None):
        """Fetch and parse the object table info for one or more object IDs.

        Args:
            object_id: An object ID to fetch information about. If this is
                None, then the entire object table is fetched.

        Returns:
            Information from the object table.
        """
        self._check_connected()
        if object_id is not None:
            # Return information about a single object ID.
            return self._object_table(object_id)
        else:
            # Return the entire object table.
            object_keys = self._keys(ray.gcs_utils.TablePrefix_OBJECT_string +
                                     "*")
            object_ids_binary = {
                key[len(ray.gcs_utils.TablePrefix_OBJECT_string):]
                for key in object_keys
            }

            results = {}
            for object_id_binary in object_ids_binary:
                results[binary_to_object_id(object_id_binary)] = (
                    self._object_table(binary_to_object_id(object_id_binary)))
            return results

    def _task_table(self, task_id):
        """Fetch and parse the task table information for a single task ID.

        Args:
            task_id_binary: A string of bytes with the task ID to get
                information about.

        Returns:
            A dictionary with information about the task ID in question.
        """
        message = self._execute_command(task_id, "RAY.TABLE_LOOKUP",
                                        ray.gcs_utils.TablePrefix.RAYLET_TASK,
                                        "", task_id.id())
        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            message, 0)

        assert gcs_entries.EntriesLength() == 1

        task_table_message = ray.gcs_utils.Task.GetRootAsTask(
            gcs_entries.Entries(0), 0)

        execution_spec = task_table_message.TaskExecutionSpec()
        task_spec = task_table_message.TaskSpecification()
        task_spec = ray.raylet.task_from_string(task_spec)
        task_spec_info = {
            "DriverID": binary_to_hex(task_spec.driver_id().id()),
            "TaskID": binary_to_hex(task_spec.task_id().id()),
            "ParentTaskID": binary_to_hex(task_spec.parent_task_id().id()),
            "ParentCounter": task_spec.parent_counter(),
            "ActorID": binary_to_hex(task_spec.actor_id().id()),
            "ActorCreationID": binary_to_hex(
                task_spec.actor_creation_id().id()),
            "ActorCreationDummyObjectID": binary_to_hex(
                task_spec.actor_creation_dummy_object_id().id()),
            "ActorCounter": task_spec.actor_counter(),
            "FunctionID": binary_to_hex(task_spec.function_id().id()),
            "Args": task_spec.arguments(),
            "ReturnObjectIDs": task_spec.returns(),
            "RequiredResources": task_spec.required_resources()
        }

        return {
            "ExecutionSpec": {
                "Dependencies": [
                    execution_spec.Dependencies(i)
                    for i in range(execution_spec.DependenciesLength())
                ],
                "LastTimestamp": execution_spec.LastTimestamp(),
                "NumForwards": execution_spec.NumForwards()
            },
            "TaskSpec": task_spec_info
        }

    def task_table(self, task_id=None):
        """Fetch and parse the task table information for one or more task IDs.

        Args:
            task_id: A hex string of the task ID to fetch information about. If
                this is None, then the task object table is fetched.

        Returns:
            Information from the task table.
        """
        self._check_connected()
        if task_id is not None:
            task_id = ray.ObjectID(hex_to_binary(task_id))
            return self._task_table(task_id)
        else:
            task_table_keys = self._keys(
                ray.gcs_utils.TablePrefix_RAYLET_TASK_string + "*")
            task_ids_binary = [
                key[len(ray.gcs_utils.TablePrefix_RAYLET_TASK_string):]
                for key in task_table_keys
            ]

            results = {}
            for task_id_binary in task_ids_binary:
                results[binary_to_hex(task_id_binary)] = self._task_table(
                    ray.ObjectID(task_id_binary))
            return results

    def function_table(self, function_id=None):
        """Fetch and parse the function table.

        Returns:
            A dictionary that maps function IDs to information about the
                function.
        """
        self._check_connected()
        function_table_keys = self.redis_client.keys(
            ray.gcs_utils.FUNCTION_PREFIX + "*")
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

        NIL_CLIENT_ID = ray_constants.ID_SIZE * b"\xff"
        message = self.redis_client.execute_command(
            "RAY.TABLE_LOOKUP", ray.gcs_utils.TablePrefix.CLIENT, "",
            NIL_CLIENT_ID)

        # Handle the case where no clients are returned. This should only
        # occur potentially immediately after the cluster is started.
        if message is None:
            return []

        node_info = {}
        gcs_entry = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            message, 0)

        # Since GCS entries are append-only, we override so that
        # only the latest entries are kept.
        for i in range(gcs_entry.EntriesLength()):
            client = (ray.gcs_utils.ClientTableData.GetRootAsClientTableData(
                gcs_entry.Entries(i), 0))

            resources = {
                decode(client.ResourcesTotalLabel(i)):
                client.ResourcesTotalCapacity(i)
                for i in range(client.ResourcesTotalLabelLength())
            }
            client_id = ray.utils.binary_to_hex(client.ClientId())

            # If this client is being removed, then it must
            # have previously been inserted, and
            # it cannot have previously been removed.
            if not client.IsInsertion():
                assert client_id in node_info, "Client removed not found!"
                assert node_info[client_id]["IsInsertion"], (
                    "Unexpected duplicate removal of client.")

            node_info[client_id] = {
                "ClientID": client_id,
                "IsInsertion": client.IsInsertion(),
                "NodeManagerAddress": decode(client.NodeManagerAddress()),
                "NodeManagerPort": client.NodeManagerPort(),
                "ObjectManagerPort": client.ObjectManagerPort(),
                "ObjectStoreSocketName": decode(
                    client.ObjectStoreSocketName()),
                "RayletSocketName": decode(client.RayletSocketName()),
                "Resources": resources
            }
        return list(node_info.values())

    def log_files(self):
        """Fetch and return a dictionary of log file names to outputs.

        Returns:
            IP address to log file name to log file contents mappings.
        """
        relevant_files = self.redis_client.keys("LOGFILE*")

        ip_filename_file = {}

        for filename in relevant_files:
            filename = decode(filename)
            filename_components = filename.split(":")
            ip_addr = filename_components[1]

            file = self.redis_client.lrange(filename, 0, -1)
            file_str = []
            for x in file:
                y = decode(x)
                file_str.append(y)

            if ip_addr not in ip_filename_file:
                ip_filename_file[ip_addr] = {}

            ip_filename_file[ip_addr][filename] = file_str

        return ip_filename_file

    def task_profiles(self, num_tasks, start=None, end=None, fwd=True):
        """Fetch and return a list of task profiles.

        Args:
            num_tasks: A limit on the number of tasks that task_profiles will
                return.
            start: The start point of the time window that is queried for
                tasks.
            end: The end point in time of the time window that is queried for
                tasks.
            fwd: If True, means that zrange will be used. If False, zrevrange.
                This argument is only meaningful in conjunction with the
                num_tasks argument. This controls whether the tasks returned
                are the most recent or the least recent.

        Returns:
            A tuple of two elements. The first element is a dictionary mapping
                the task ID of a task to a list of the profiling information
                for all of the executions of that task. The second element is a
                list of profiling information for tasks where the events have
                no task ID.
        """
        task_info = {}
        event_log_sets = self.redis_client.keys("event_log*")

        # The heap is used to maintain the set of x tasks that occurred the
        # most recently across all of the workers, where x is defined as the
        # function parameter num. The key is the start time of the "get_task"
        # component of each task. Calling heappop will result in the task with
        # the earliest "get_task_start" to be removed from the heap.
        heap = []
        heapq.heapify(heap)
        heap_size = 0

        # Set up a param dict to pass the redis command
        params = {"withscores": True}
        if start is not None:
            params["min"] = start
        elif end is not None:
            params["min"] = 0

        if end is not None:
            params["max"] = end
        elif start is not None:
            params["max"] = time.time()

        if start is None and end is None:
            params["end"] = num_tasks - 1
        else:
            params["num"] = num_tasks
        params["start"] = 0

        # Parse through event logs to determine task start and end points.
        for event_log_set in event_log_sets:
            if start is None and end is None:
                if fwd:
                    event_list = self.redis_client.zrange(
                        event_log_set, **params)
                else:
                    event_list = self.redis_client.zrevrange(
                        event_log_set, **params)
            else:
                if fwd:
                    event_list = self.redis_client.zrangebyscore(
                        event_log_set, **params)
                else:
                    event_list = self.redis_client.zrevrangebyscore(
                        event_log_set, **params)

            for (event, score) in event_list:
                event_dict = json.loads(decode(event))
                task_id = ""
                for event in event_dict:
                    if "task_id" in event[3]:
                        task_id = event[3]["task_id"]
                task_info[task_id] = {}
                task_info[task_id]["score"] = score
                # Add task to (min/max) heap by its start point.
                # if fwd, we want to delete the largest elements, so -score
                heapq.heappush(heap, (-score if fwd else score, task_id))
                heap_size += 1

                for event in event_dict:
                    if event[1] == "get_task" and event[2] == 1:
                        task_info[task_id]["get_task_start"] = event[0]
                    if event[1] == "get_task" and event[2] == 2:
                        task_info[task_id]["get_task_end"] = event[0]
                    if (event[1] == "register_remote_function"
                            and event[2] == 1):
                        task_info[task_id]["import_remote_start"] = event[0]
                    if (event[1] == "register_remote_function"
                            and event[2] == 2):
                        task_info[task_id]["import_remote_end"] = event[0]
                    if (event[1] == "task:deserialize_arguments"
                            and event[2] == 1):
                        task_info[task_id]["get_arguments_start"] = event[0]
                    if (event[1] == "task:deserialize_arguments"
                            and event[2] == 2):
                        task_info[task_id]["get_arguments_end"] = event[0]
                    if event[1] == "task:execute" and event[2] == 1:
                        task_info[task_id]["execute_start"] = event[0]
                    if event[1] == "task:execute" and event[2] == 2:
                        task_info[task_id]["execute_end"] = event[0]
                    if event[1] == "task:store_outputs" and event[2] == 1:
                        task_info[task_id]["store_outputs_start"] = event[0]
                    if event[1] == "task:store_outputs" and event[2] == 2:
                        task_info[task_id]["store_outputs_end"] = event[0]
                    if "worker_id" in event[3]:
                        task_info[task_id]["worker_id"] = event[3]["worker_id"]
                    if "function_name" in event[3]:
                        task_info[task_id]["function_name"] = (
                            event[3]["function_name"])

                if heap_size > num_tasks:
                    min_task, task_id_hex = heapq.heappop(heap)
                    del task_info[task_id_hex]
                    heap_size -= 1

        for key, info in task_info.items():
            self._add_missing_timestamps(info)

        return task_info

    def _profile_table(self, component_id):
        """Get the profile events for a given component.

        Args:
            component_id: An identifier for a component.

        Returns:
            A list of the profile events for the specified process.
        """
        # TODO(rkn): This method should support limiting the number of log
        # events and should also support returning a window of events.
        message = self._execute_command(component_id, "RAY.TABLE_LOOKUP",
                                        ray.gcs_utils.TablePrefix.PROFILE, "",
                                        component_id.id())

        if message is None:
            return []

        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            message, 0)

        profile_events = []
        for i in range(gcs_entries.EntriesLength()):
            profile_table_message = (
                ray.gcs_utils.ProfileTableData.GetRootAsProfileTableData(
                    gcs_entries.Entries(i), 0))

            component_type = decode(profile_table_message.ComponentType())
            component_id = binary_to_hex(profile_table_message.ComponentId())
            node_ip_address = decode(profile_table_message.NodeIpAddress())

            for j in range(profile_table_message.ProfileEventsLength()):
                profile_event_message = profile_table_message.ProfileEvents(j)

                profile_event = {
                    "event_type": decode(profile_event_message.EventType()),
                    "component_id": component_id,
                    "node_ip_address": node_ip_address,
                    "component_type": component_type,
                    "start_time": profile_event_message.StartTime(),
                    "end_time": profile_event_message.EndTime(),
                    "extra_data": json.loads(
                        decode(profile_event_message.ExtraData())),
                }

                profile_events.append(profile_event)

        return profile_events

    def profile_table(self):
        profile_table_keys = self._keys(
            ray.gcs_utils.TablePrefix_PROFILE_string + "*")
        component_identifiers_binary = [
            key[len(ray.gcs_utils.TablePrefix_PROFILE_string):]
            for key in profile_table_keys
        ]

        return {
            binary_to_hex(component_id): self._profile_table(
                binary_to_object_id(component_id))
            for component_id in component_identifiers_binary
        }

    def _seconds_to_microseconds(self, time_in_seconds):
        """A helper function for converting seconds to microseconds."""
        time_in_microseconds = 10**6 * time_in_seconds
        return time_in_microseconds

    # Colors are specified at
    # https://github.com/catapult-project/catapult/blob/master/tracing/tracing/base/color_scheme.html.  # noqa: E501
    _default_color_mapping = defaultdict(
        lambda: "generic_work", {
            "worker_idle": "cq_build_abandoned",
            "task": "rail_response",
            "task:deserialize_arguments": "rail_load",
            "task:execute": "rail_animation",
            "task:store_outputs": "rail_idle",
            "wait_for_function": "detailed_memory_dump",
            "ray.get": "good",
            "ray.put": "terrible",
            "ray.wait": "vsync_highlight_color",
            "submit_task": "background_memory_dump",
            "fetch_and_run_function": "detailed_memory_dump",
            "register_remote_function": "detailed_memory_dump",
        })

    # These colors are for use in Chrome tracing.
    _chrome_tracing_colors = [
        "thread_state_uninterruptible",
        "thread_state_iowait",
        "thread_state_running",
        "thread_state_runnable",
        "thread_state_sleeping",
        "thread_state_unknown",
        "background_memory_dump",
        "light_memory_dump",
        "detailed_memory_dump",
        "vsync_highlight_color",
        "generic_work",
        "good",
        "bad",
        "terrible",
        # "black",
        # "grey",
        # "white",
        "yellow",
        "olive",
        "rail_response",
        "rail_animation",
        "rail_idle",
        "rail_load",
        "startup",
        "heap_dump_stack_frame",
        "heap_dump_object_type",
        "heap_dump_child_node_arrow",
        "cq_build_running",
        "cq_build_passed",
        "cq_build_failed",
        "cq_build_abandoned",
        "cq_build_attempt_runnig",
        "cq_build_attempt_passed",
        "cq_build_attempt_failed",
    ]

    def chrome_tracing_dump(self, filename=None):
        """Return a list of profiling events that can viewed as a timeline.

        To view this information as a timeline, simply dump it as a json file
        by passing in "filename" or using using json.dump, and then load go to
        chrome://tracing in the Chrome web browser and load the dumped file.
        Make sure to enable "Flow events" in the "View Options" menu.

        Args:
            filename: If a filename is provided, the timeline is dumped to that
                file.

        Returns:
            If filename is not provided, this returns a list of profiling
                events. Each profile event is a dictionary.
        """
        # TODO(rkn): Support including the task specification data in the
        # timeline.
        # TODO(rkn): This should support viewing just a window of time or a
        # limited number of events.

        profile_table = self.profile_table()
        all_events = []

        for component_id_hex, component_events in profile_table.items():
            # Only consider workers and drivers.
            component_type = component_events[0]["component_type"]
            if component_type not in ["worker", "driver"]:
                continue

            for event in component_events:
                new_event = {
                    # The category of the event.
                    "cat": event["event_type"],
                    # The string displayed on the event.
                    "name": event["event_type"],
                    # The identifier for the group of rows that the event
                    # appears in.
                    "pid": event["node_ip_address"],
                    # The identifier for the row that the event appears in.
                    "tid": event["component_type"] + ":" +
                    event["component_id"],
                    # The start time in microseconds.
                    "ts": self._seconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._seconds_to_microseconds(event["end_time"] -
                                                         event["start_time"]),
                    # What is this?
                    "ph": "X",
                    # This is the name of the color to display the box in.
                    "cname": self._default_color_mapping[event["event_type"]],
                    # The extra user-defined data.
                    "args": event["extra_data"],
                }

                # Modify the json with the additional user-defined extra data.
                # This can be used to add fields or override existing fields.
                if "cname" in event["extra_data"]:
                    new_event["cname"] = event["extra_data"]["cname"]
                if "name" in event["extra_data"]:
                    new_event["name"] = event["extra_data"]["name"]

                all_events.append(new_event)

        if filename is not None:
            with open(filename, "w") as outfile:
                json.dump(all_events, outfile)
        else:
            return all_events

    def chrome_tracing_object_transfer_dump(self, filename=None):
        """Return a list of transfer events that can viewed as a timeline.

        To view this information as a timeline, simply dump it as a json file
        by passing in "filename" or using using json.dump, and then load go to
        chrome://tracing in the Chrome web browser and load the dumped file.
        Make sure to enable "Flow events" in the "View Options" menu.

        Args:
            filename: If a filename is provided, the timeline is dumped to that
                file.

        Returns:
            If filename is not provided, this returns a list of profiling
                events. Each profile event is a dictionary.
        """
        client_id_to_address = {}
        for client_info in ray.global_state.client_table():
            client_id_to_address[client_info["ClientID"]] = "{}:{}".format(
                client_info["NodeManagerAddress"],
                client_info["ObjectManagerPort"])

        all_events = []

        for key, items in self.profile_table().items():
            # Only consider object manager events.
            if items[0]["component_type"] != "object_manager":
                continue

            for event in items:
                if event["event_type"] == "transfer_send":
                    object_id, remote_client_id, _, _ = event["extra_data"]

                elif event["event_type"] == "transfer_receive":
                    object_id, remote_client_id, _, _ = event["extra_data"]

                elif event["event_type"] == "receive_pull_request":
                    object_id, remote_client_id = event["extra_data"]

                else:
                    assert False, "This should be unreachable."

                # Choose a color by reading the first couple of hex digits of
                # the object ID as an integer and turning that into a color.
                object_id_int = int(object_id[:2], 16)
                color = self._chrome_tracing_colors[object_id_int % len(
                    self._chrome_tracing_colors)]

                new_event = {
                    # The category of the event.
                    "cat": event["event_type"],
                    # The string displayed on the event.
                    "name": event["event_type"],
                    # The identifier for the group of rows that the event
                    # appears in.
                    "pid": client_id_to_address[key],
                    # The identifier for the row that the event appears in.
                    "tid": client_id_to_address[remote_client_id],
                    # The start time in microseconds.
                    "ts": self._seconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._seconds_to_microseconds(event["end_time"] -
                                                         event["start_time"]),
                    # What is this?
                    "ph": "X",
                    # This is the name of the color to display the box in.
                    "cname": color,
                    # The extra user-defined data.
                    "args": event["extra_data"],
                }
                all_events.append(new_event)

                # Add another box with a color indicating whether it was a send
                # or a receive event.
                if event["event_type"] == "transfer_send":
                    additional_event = new_event.copy()
                    additional_event["cname"] = "black"
                    all_events.append(additional_event)
                elif event["event_type"] == "transfer_receive":
                    additional_event = new_event.copy()
                    additional_event["cname"] = "grey"
                    all_events.append(additional_event)
                else:
                    pass

        if filename is not None:
            with open(filename, "w") as outfile:
                json.dump(all_events, outfile)
        else:
            return all_events

    def dump_catapult_trace(self,
                            path,
                            task_info,
                            breakdowns=True,
                            task_dep=True,
                            obj_dep=True):
        """Dump task profiling information to a file.

        This information can be viewed as a timeline of profiling information
        by going to chrome://tracing in the chrome web browser and loading the
        appropriate file.

        Args:
            path: The filepath to dump the profiling information to.
            task_info: The task info to use to generate the trace. Should be
                the output of ray.global_state.task_profiles().
            breakdowns: Boolean indicating whether to break down the tasks into
                more fine-grained segments.
            task_dep: Boolean indicating whether or not task submission edges
                should be included in the trace.
            obj_dep: Boolean indicating whether or not object dependency edges
                should be included in the trace.
        """
        workers = self.workers()

        task_table = {}
        # TODO(ekl) reduce the number of RPCs here with MGET
        for task_id, _ in task_info.items():
            try:
                # TODO (hme): do something to correct slider here,
                # slider should be correct to begin with, though.
                task_table[task_id] = self.task_table(task_id)
                task_table[task_id]["TaskSpec"]["Args"] = [
                    repr(arg)
                    for arg in task_table[task_id]["TaskSpec"]["Args"]
                ]
            except Exception:
                print("Could not find task {}".format(task_id))

        # filter out tasks not in task_table
        task_info = {k: v for k, v in task_info.items() if k in task_table}

        start_time = None
        for info in task_info.values():
            task_start = min(self._get_times(info))
            if not start_time or task_start < start_time:
                start_time = task_start

        def micros(ts):
            return int(1e6 * ts)

        def micros_rel(ts):
            return micros(ts - start_time)

        seen_obj = {}

        full_trace = []
        for task_id, info in task_info.items():
            worker = workers[info["worker_id"]]
            task_t_info = task_table[task_id]

            # The total_info dictionary is what is displayed when selecting a
            # task in the timeline. We copy the task spec so that we don't
            # modify it in place since we will use the original values later.
            total_info = copy.copy(task_table[task_id]["TaskSpec"])
            total_info["Args"] = [
                oid.hex() if isinstance(oid, ray.ObjectID) else oid
                for oid in task_t_info["TaskSpec"]["Args"]
            ]
            total_info["ReturnObjectIDs"] = [
                oid.hex() for oid in task_t_info["TaskSpec"]["ReturnObjectIDs"]
            ]
            total_info["LocalSchedulerID"] = task_t_info["LocalSchedulerID"]
            total_info["get_arguments"] = (
                info["get_arguments_end"] - info["get_arguments_start"])
            total_info["execute"] = (
                info["execute_end"] - info["execute_start"])
            total_info["store_outputs"] = (
                info["store_outputs_end"] - info["store_outputs_start"])
            total_info["function_name"] = info["function_name"]
            total_info["worker_id"] = info["worker_id"]

            parent_info = task_info.get(
                task_table[task_id]["TaskSpec"]["ParentTaskID"])
            worker = workers[info["worker_id"]]
            # The catapult trace format documentation can be found here:
            # https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview  # noqa: E501
            if breakdowns:
                if "get_arguments_end" in info:
                    get_args_trace = {
                        "cat": "get_arguments",
                        "pid": "Node " + worker["node_ip_address"],
                        "tid": info["worker_id"],
                        "id": task_id,
                        "ts": micros_rel(info["get_arguments_start"]),
                        "ph": "X",
                        "name": info["function_name"] + ":get_arguments",
                        "args": total_info,
                        "dur": micros(info["get_arguments_end"] -
                                      info["get_arguments_start"]),
                        "cname": "rail_idle"
                    }
                    full_trace.append(get_args_trace)

                if "store_outputs_end" in info:
                    outputs_trace = {
                        "cat": "store_outputs",
                        "pid": "Node " + worker["node_ip_address"],
                        "tid": info["worker_id"],
                        "id": task_id,
                        "ts": micros_rel(info["store_outputs_start"]),
                        "ph": "X",
                        "name": info["function_name"] + ":store_outputs",
                        "args": total_info,
                        "dur": micros(info["store_outputs_end"] -
                                      info["store_outputs_start"]),
                        "cname": "thread_state_runnable"
                    }
                    full_trace.append(outputs_trace)

                if "execute_end" in info:
                    execute_trace = {
                        "cat": "execute",
                        "pid": "Node " + worker["node_ip_address"],
                        "tid": info["worker_id"],
                        "id": task_id,
                        "ts": micros_rel(info["execute_start"]),
                        "ph": "X",
                        "name": info["function_name"] + ":execute",
                        "args": total_info,
                        "dur": micros(info["execute_end"] -
                                      info["execute_start"]),
                        "cname": "rail_animation"
                    }
                    full_trace.append(execute_trace)

            else:
                if parent_info:
                    parent_worker = workers[parent_info["worker_id"]]
                    parent_times = self._get_times(parent_info)
                    parent_profile = task_info.get(
                        task_table[task_id]["TaskSpec"]["ParentTaskID"])

                    _parent_id = parent_info["worker_id"] + str(
                        micros(min(parent_times)))

                    parent = {
                        "cat": "submit_task",
                        "pid": "Node " + parent_worker["node_ip_address"],
                        "tid": parent_info["worker_id"],
                        "ts": micros_rel(
                            parent_profile
                            and parent_profile["get_arguments_start"]
                            or start_time),
                        "ph": "s",
                        "name": "SubmitTask",
                        "args": {},
                        "id": _parent_id,
                    }
                    full_trace.append(parent)

                    _id = info["worker_id"] + str(micros(min(parent_times)))

                    task_trace = {
                        "cat": "submit_task",
                        "pid": "Node " + worker["node_ip_address"],
                        "tid": info["worker_id"],
                        "ts": micros_rel(info["get_arguments_start"]),
                        "ph": "f",
                        "name": "SubmitTask",
                        "args": {},
                        "id": _id,
                        "bp": "e",
                        "cname": "olive"
                    }
                    full_trace.append(task_trace)

                task = {
                    "cat": "task",
                    "pid": "Node " + worker["node_ip_address"],
                    "tid": info["worker_id"],
                    "id": task_id,
                    "ts": micros_rel(info["get_arguments_start"]),
                    "ph": "X",
                    "name": info["function_name"],
                    "args": total_info,
                    "dur": micros(info["store_outputs_end"] -
                                  info["get_arguments_start"]),
                    "cname": "thread_state_runnable"
                }
                full_trace.append(task)

            if task_dep:
                if parent_info:
                    parent_worker = workers[parent_info["worker_id"]]
                    parent_times = self._get_times(parent_info)
                    parent_profile = task_info.get(
                        task_table[task_id]["TaskSpec"]["ParentTaskID"])

                    _parent_id = parent_info["worker_id"] + str(
                        micros(min(parent_times)))

                    parent = {
                        "cat": "submit_task",
                        "pid": "Node " + parent_worker["node_ip_address"],
                        "tid": parent_info["worker_id"],
                        "ts": micros_rel(
                            parent_profile
                            and parent_profile["get_arguments_start"]
                            or start_time),
                        "ph": "s",
                        "name": "SubmitTask",
                        "args": {},
                        "id": _parent_id,
                    }
                    full_trace.append(parent)

                    _id = info["worker_id"] + str(micros(min(parent_times)))

                    task_trace = {
                        "cat": "submit_task",
                        "pid": "Node " + worker["node_ip_address"],
                        "tid": info["worker_id"],
                        "ts": micros_rel(info["get_arguments_start"]),
                        "ph": "f",
                        "name": "SubmitTask",
                        "args": {},
                        "id": _id,
                        "bp": "e"
                    }
                    full_trace.append(task_trace)

            if obj_dep:
                args = task_table[task_id]["TaskSpec"]["Args"]
                for arg in args:
                    # Don't visualize arguments that are not object IDs.
                    if isinstance(arg, ray.ObjectID):
                        object_info = self._object_table(arg)
                        # Don't visualize objects that were created by calls to
                        # put.
                        if not object_info["IsPut"]:
                            if arg not in seen_obj:
                                seen_obj[arg] = 0
                            seen_obj[arg] += 1
                            owner_task = self._object_table(arg)["TaskID"]
                            if owner_task in task_info:
                                owner_worker = (workers[task_info[owner_task][
                                    "worker_id"]])
                                # Adding/subtracting 2 to the time associated
                                # with the beginning/ending of the flow event
                                # is necessary to make the flow events show up
                                # reliably. When these times are exact, this is
                                # presumably an edge case, and catapult doesn't
                                # recognize that there is a duration event at
                                # that exact point in time that the flow event
                                # should be bound to. This issue is solved by
                                # adding the 2 ms to the start/end time of the
                                # flow event, which guarantees overlap with the
                                # duration event that it's associated with, and
                                # the flow event therefore always gets drawn.
                                owner = {
                                    "cat": "obj_dependency",
                                    "pid": ("Node " +
                                            owner_worker["node_ip_address"]),
                                    "tid": task_info[owner_task]["worker_id"],
                                    "ts": micros_rel(task_info[owner_task]
                                                     ["store_outputs_end"]) -
                                    2,
                                    "ph": "s",
                                    "name": "ObjectDependency",
                                    "args": {},
                                    "bp": "e",
                                    "cname": "cq_build_attempt_failed",
                                    "id": "obj" + str(arg) + str(seen_obj[arg])
                                }
                                full_trace.append(owner)

                            dependent = {
                                "cat": "obj_dependency",
                                "pid": "Node " + worker["node_ip_address"],
                                "tid": info["worker_id"],
                                "ts": micros_rel(info["get_arguments_start"]) +
                                2,
                                "ph": "f",
                                "name": "ObjectDependency",
                                "args": {},
                                "cname": "cq_build_attempt_failed",
                                "bp": "e",
                                "id": "obj" + str(arg) + str(seen_obj[arg])
                            }
                            full_trace.append(dependent)

        print("Creating JSON {}/{}".format(len(full_trace), len(task_info)))
        with open(path, "w") as outfile:
            json.dump(full_trace, outfile)

    def _get_times(self, data):
        """Extract the numerical times from a task profile.

        This is a helper method for dump_catapult_trace.

        Args:
            data: This must be a value in the dictionary returned by the
                task_profiles function.
        """
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

    def _add_missing_timestamps(self, info):
        """Fills in any missing timestamp values in a task info.

        Task timestamps may be missing if the task fails or is partially
        executed.
        """

        keys = [
            "acquire_lock_start", "acquire_lock_end", "get_arguments_start",
            "get_arguments_end", "execute_start", "execute_end",
            "store_outputs_start", "store_outputs_end"
        ]

        latest_timestamp = 0
        for key in keys:
            cur = info.get(key, latest_timestamp)
            info[key] = cur
            latest_timestamp = cur

    def workers(self):
        """Get a dictionary mapping worker ID to worker information."""
        worker_keys = self.redis_client.keys("Worker*")
        workers_data = {}

        for worker_key in worker_keys:
            worker_info = self.redis_client.hgetall(worker_key)
            worker_id = binary_to_hex(worker_key[len("Workers:"):])

            workers_data[worker_id] = {
                "local_scheduler_socket": (decode(
                    worker_info[b"local_scheduler_socket"])),
                "node_ip_address": decode(worker_info[b"node_ip_address"]),
                "plasma_store_socket": decode(
                    worker_info[b"plasma_store_socket"])
            }
            if b"stderr_file" in worker_info:
                workers_data[worker_id]["stderr_file"] = decode(
                    worker_info[b"stderr_file"])
            if b"stdout_file" in worker_info:
                workers_data[worker_id]["stdout_file"] = decode(
                    worker_info[b"stdout_file"])
        return workers_data

    def actors(self):
        actor_keys = self.redis_client.keys("Actor:*")
        actor_info = {}
        for key in actor_keys:
            info = self.redis_client.hgetall(key)
            actor_id = key[len("Actor:"):]
            assert len(actor_id) == ray_constants.ID_SIZE
            actor_info[binary_to_hex(actor_id)] = {
                "class_id": binary_to_hex(info[b"class_id"]),
                "driver_id": binary_to_hex(info[b"driver_id"]),
                "local_scheduler_id": binary_to_hex(
                    info[b"local_scheduler_id"]),
                "num_gpus": int(info[b"num_gpus"]),
                "removed": decode(info[b"removed"]) == "True"
            }
        return actor_info

    def _job_length(self):
        event_log_sets = self.redis_client.keys("event_log*")
        overall_smallest = sys.maxsize
        overall_largest = 0
        num_tasks = 0
        for event_log_set in event_log_sets:
            fwd_range = self.redis_client.zrange(
                event_log_set, start=0, end=0, withscores=True)
            overall_smallest = min(overall_smallest, fwd_range[0][1])

            rev_range = self.redis_client.zrevrange(
                event_log_set, start=0, end=0, withscores=True)
            overall_largest = max(overall_largest, rev_range[0][1])

            num_tasks += self.redis_client.zcount(
                event_log_set, min=0, max=time.time())
        if num_tasks is 0:
            return 0, 0, 0
        return overall_smallest, overall_largest, num_tasks

    def cluster_resources(self):
        """Get the current total cluster resources.

        Note that this information can grow stale as nodes are added to or
        removed from the cluster.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        resources = defaultdict(int)
        clients = self.client_table()
        for client in clients:
            # Only count resources from live clients.
            if client["IsInsertion"]:
                for key, value in client["Resources"].items():
                    resources[key] += value

        return dict(resources)

    def _live_client_ids(self):
        """Returns a set of client IDs corresponding to clients still alive."""
        return {
            client["ClientID"]
            for client in self.client_table() if client["IsInsertion"]
        }

    def available_resources(self):
        """Get the current available cluster resources.

        This is different from `cluster_resources` in that this will return
        idle (available) resources rather than total resources.

        Note that this information can grow stale as tasks start and finish.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        available_resources_by_id = {}

        subscribe_clients = [
            redis_client.pubsub(ignore_subscribe_messages=True)
            for redis_client in self.redis_clients
        ]
        for subscribe_client in subscribe_clients:
            subscribe_client.subscribe(ray.gcs_utils.XRAY_HEARTBEAT_CHANNEL)

        client_ids = self._live_client_ids()

        while set(available_resources_by_id.keys()) != client_ids:
            for subscribe_client in subscribe_clients:
                # Parse client message
                raw_message = subscribe_client.get_message()
                if (raw_message is None or raw_message["channel"] !=
                        ray.gcs_utils.XRAY_HEARTBEAT_CHANNEL):
                    continue
                data = raw_message["data"]
                gcs_entries = (
                    ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
                        data, 0))
                heartbeat_data = gcs_entries.Entries(0)
                message = (ray.gcs_utils.HeartbeatTableData.
                           GetRootAsHeartbeatTableData(heartbeat_data, 0))
                # Calculate available resources for this client
                num_resources = message.ResourcesAvailableLabelLength()
                dynamic_resources = {}
                for i in range(num_resources):
                    resource_id = decode(message.ResourcesAvailableLabel(i))
                    dynamic_resources[resource_id] = (
                        message.ResourcesAvailableCapacity(i))

                # Update available resources for this client
                client_id = ray.utils.binary_to_hex(message.ClientId())
                available_resources_by_id[client_id] = dynamic_resources

            # Update clients in cluster
            client_ids = self._live_client_ids()

            # Remove disconnected clients
            for client_id in available_resources_by_id.keys():
                if client_id not in client_ids:
                    del available_resources_by_id[client_id]

        # Calculate total available resources
        total_available_resources = defaultdict(int)
        for available_resources in available_resources_by_id.values():
            for resource_id, num_available in available_resources.items():
                total_available_resources[resource_id] += num_available

        return dict(total_available_resources)

    def _error_messages(self, job_id):
        """Get the error messages for a specific job.

        Args:
            job_id: The ID of the job to get the errors for.

        Returns:
            A list of the error messages for this job.
        """
        message = self.redis_client.execute_command(
            "RAY.TABLE_LOOKUP", ray.gcs_utils.TablePrefix.ERROR_INFO, "",
            job_id.id())

        # If there are no errors, return early.
        if message is None:
            return []

        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            message, 0)
        error_messages = []
        for i in range(gcs_entries.EntriesLength()):
            error_data = ray.gcs_utils.ErrorTableData.GetRootAsErrorTableData(
                gcs_entries.Entries(i), 0)
            assert job_id.id() == error_data.JobId()
            error_message = {
                "type": decode(error_data.Type()),
                "message": decode(error_data.ErrorMessage()),
                "timestamp": error_data.Timestamp(),
            }
            error_messages.append(error_message)
        return error_messages

    def error_messages(self, job_id=None):
        """Get the error messages for all jobs or a specific job.

        Args:
            job_id: The specific job to get the errors for. If this is None,
                then this method retrieves the errors for all jobs.

        Returns:
            A dictionary mapping job ID to a list of the error messages for
                that job.
        """
        if job_id is not None:
            return self._error_messages(job_id)

        error_table_keys = self.redis_client.keys(
            ray.gcs_utils.TablePrefix_ERROR_INFO_string + "*")
        job_ids = [
            key[len(ray.gcs_utils.TablePrefix_ERROR_INFO_string):]
            for key in error_table_keys
        ]

        return {
            binary_to_hex(job_id): self._error_messages(ray.ObjectID(job_id))
            for job_id in job_ids
        }
