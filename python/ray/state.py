from collections import defaultdict
import json
import logging
import sys
import time

import ray

from ray import (
    gcs_utils,
    services,
)
from ray.utils import (decode, binary_to_hex, hex_to_binary)

from ray._raylet import GlobalStateAccessor

logger = logging.getLogger(__name__)


class GlobalState:
    """A class used to interface with the Ray control state.

    # TODO(zongheng): In the future move this to use Ray's redis module in the
    # backend to cut down on # of request RPCs.

    Attributes:
        redis_client: The Redis client used to query the primary redis server.
        redis_clients: Redis clients for each of the Redis shards.
        global_state_accessor: The client used to query gcs table from gcs
            server.
    """

    def __init__(self):
        """Create a GlobalState object."""
        # The redis server storing metadata, such as function table, client
        # table, log files, event logs, workers/actions info.
        self.redis_client = None
        # Clients for the redis shards, storing the object table & task table.
        self.redis_clients = None
        self.global_state_accessor = None

    def _check_connected(self):
        """Check that the object has been initialized before it is used.

        Raises:
            RuntimeError: An exception is raised if ray.init() has not been
                called yet.
        """
        if self.redis_client is None:
            raise RuntimeError("The ray global state API cannot be used "
                               "before ray.init has been called.")

        if self.redis_clients is None:
            raise RuntimeError("The ray global state API cannot be used "
                               "before ray.init has been called.")

        if self.global_state_accessor is None:
            raise RuntimeError("The ray global state API cannot be used "
                               "before ray.init has been called.")

    def disconnect(self):
        """Disconnect global state from GCS."""
        self.redis_client = None
        self.redis_clients = None
        if self.global_state_accessor is not None:
            self.global_state_accessor.disconnect()
            self.global_state_accessor = None

    def _initialize_global_state(self,
                                 redis_address,
                                 redis_password=None,
                                 timeout=20):
        """Initialize the GlobalState object by connecting to Redis.

        It's possible that certain keys in Redis may not have been fully
        populated yet. In this case, we will retry this method until they have
        been populated or we exceed a timeout.

        Args:
            redis_address: The Redis address to connect.
            redis_password: The password of the redis server.
        """
        self.redis_client = services.create_redis_client(
            redis_address, redis_password)
        self.global_state_accessor = GlobalStateAccessor(
            redis_address, redis_password, False)
        self.global_state_accessor.connect()
        start_time = time.time()

        num_redis_shards = None
        redis_shard_addresses = []

        while time.time() - start_time < timeout:
            # Attempt to get the number of Redis shards.
            num_redis_shards = self.redis_client.get("NumRedisShards")
            if num_redis_shards is None:
                print("Waiting longer for NumRedisShards to be populated.")
                time.sleep(1)
                continue
            num_redis_shards = int(num_redis_shards)
            assert num_redis_shards >= 1, (
                "Expected at least one Redis "
                "shard, found {}.".format(num_redis_shards))

            # Attempt to get all of the Redis shards.
            redis_shard_addresses = self.redis_client.lrange(
                "RedisShards", start=0, end=-1)
            if len(redis_shard_addresses) != num_redis_shards:
                print("Waiting longer for RedisShards to be populated.")
                time.sleep(1)
                continue

            # If we got here then we successfully got all of the information.
            break

        # Check to see if we timed out.
        if time.time() - start_time >= timeout:
            raise TimeoutError("Timed out while attempting to initialize the "
                               "global state. num_redis_shards = {}, "
                               "redis_shard_addresses = {}".format(
                                   num_redis_shards, redis_shard_addresses))

        # Get the rest of the information.
        self.redis_clients = []
        for shard_address in redis_shard_addresses:
            self.redis_clients.append(
                services.create_redis_client(shard_address.decode(),
                                             redis_password))

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
            object_id = ray.ObjectID(hex_to_binary(object_id))
            object_info = self.global_state_accessor.get_object_info(object_id)
            if object_info is None:
                return {}
            else:
                object_location_info = gcs_utils.ObjectLocationInfo.FromString(
                    object_info)
                return self._gen_object_info(object_location_info)
        else:
            object_table = self.global_state_accessor.get_object_table()
            results = {}
            for i in range(len(object_table)):
                object_location_info = gcs_utils.ObjectLocationInfo.FromString(
                    object_table[i])
                results[binary_to_hex(object_location_info.object_id)] = \
                    self._gen_object_info(object_location_info)
            return results

    def _gen_object_info(self, object_location_info):
        """Parse object location info.
        Returns:
            Information from object.
        """
        locations = []
        for location in object_location_info.locations:
            locations.append(ray.utils.binary_to_hex(location.manager))

        object_info = {
            "ObjectID": ray.utils.binary_to_hex(
                object_location_info.object_id),
            "Locations": locations,
        }
        return object_info

    def actor_table(self, actor_id):
        """Fetch and parse the actor table information for a single actor ID.

        Args:
            actor_id: A hex string of the actor ID to fetch information about.
                If this is None, then the actor table is fetched.

        Returns:
            Information from the actor table.
        """
        self._check_connected()

        if actor_id is not None:
            actor_id = ray.ActorID(hex_to_binary(actor_id))
            actor_info = self.global_state_accessor.get_actor_info(actor_id)
            if actor_info is None:
                return {}
            else:
                actor_table_data = gcs_utils.ActorTableData.FromString(
                    actor_info)
                return self._gen_actor_info(actor_table_data)
        else:
            actor_table = self.global_state_accessor.get_actor_table()
            results = {}
            for i in range(len(actor_table)):
                actor_table_data = gcs_utils.ActorTableData.FromString(
                    actor_table[i])
                results[binary_to_hex(actor_table_data.actor_id)] = \
                    self._gen_actor_info(actor_table_data)

            return results

    def _gen_actor_info(self, actor_table_data):
        """Parse actor table data.

        Returns:
            Information from actor table.
        """
        actor_info = {
            "ActorID": binary_to_hex(actor_table_data.actor_id),
            "JobID": binary_to_hex(actor_table_data.job_id),
            "Address": {
                "IPAddress": actor_table_data.address.ip_address,
                "Port": actor_table_data.address.port
            },
            "OwnerAddress": {
                "IPAddress": actor_table_data.owner_address.ip_address,
                "Port": actor_table_data.owner_address.port
            },
            "State": actor_table_data.state,
            "Timestamp": actor_table_data.timestamp,
        }
        return actor_info

    def node_resource_table(self, node_id=None):
        """Fetch and parse the node resource table info for one.

        Args:
            node_id: An node ID to fetch information about.

        Returns:
            Information from the node resource table.
        """
        self._check_connected()

        node_id = ray.ClientID(hex_to_binary(node_id))
        node_resource_bytes = \
            self.global_state_accessor.get_node_resource_info(node_id)
        if node_resource_bytes is None:
            return {}
        else:
            node_resource_info = gcs_utils.ResourceMap.FromString(
                node_resource_bytes)
            return {
                key: value.resource_capacity
                for key, value in node_resource_info.items.items()
            }

    def node_table(self):
        """Fetch and parse the Gcs node info table.

        Returns:
            Information about the node in the cluster.
        """
        self._check_connected()

        node_table = self.global_state_accessor.get_node_table()

        results = []
        for node_info_item in node_table:
            item = gcs_utils.GcsNodeInfo.FromString(node_info_item)
            node_info = {
                "NodeID": ray.utils.binary_to_hex(item.node_id),
                "Alive": item.state ==
                gcs_utils.GcsNodeInfo.GcsNodeState.Value("ALIVE"),
                "NodeManagerAddress": item.node_manager_address,
                "NodeManagerHostname": item.node_manager_hostname,
                "NodeManagerPort": item.node_manager_port,
                "ObjectManagerPort": item.object_manager_port,
                "ObjectStoreSocketName": item.object_store_socket_name,
                "RayletSocketName": item.raylet_socket_name
            }
            node_info["alive"] = node_info["Alive"]
            node_info["Resources"] = self.node_resource_table(
                node_info["NodeID"]) if node_info["Alive"] else {}
            results.append(node_info)
        return results

    def job_table(self):
        """Fetch and parse the Redis job table.

        Returns:
            Information about the Ray jobs in the cluster,
            namely a list of dicts with keys:
            - "JobID" (identifier for the job),
            - "DriverIPAddress" (IP address of the driver for this job),
            - "DriverPid" (process ID of the driver for this job),
            - "StartTime" (UNIX timestamp of the start time of this job),
            - "StopTime" (UNIX timestamp of the stop time of this job, if any)
        """
        self._check_connected()

        job_table = self.global_state_accessor.get_job_table()

        results = []
        for i in range(len(job_table)):
            entry = gcs_utils.JobTableData.FromString(job_table[i])
            job_info = {}
            job_info["JobID"] = entry.job_id.hex()
            job_info["DriverIPAddress"] = entry.driver_ip_address
            job_info["DriverPid"] = entry.driver_pid
            if entry.is_dead:
                job_info["StopTime"] = entry.timestamp
            else:
                job_info["StartTime"] = entry.timestamp
            results.append(job_info)

        return results

    def profile_table(self):
        self._check_connected()

        result = defaultdict(list)
        profile_table = self.global_state_accessor.get_profile_table()
        for i in range(len(profile_table)):
            profile = gcs_utils.ProfileTableData.FromString(profile_table[i])

            component_type = profile.component_type
            component_id = binary_to_hex(profile.component_id)
            node_ip_address = profile.node_ip_address

            for event in profile.profile_events:
                try:
                    extra_data = json.loads(event.extra_data)
                except ValueError:
                    extra_data = {}
                profile_event = {
                    "event_type": event.event_type,
                    "component_id": component_id,
                    "node_ip_address": node_ip_address,
                    "component_type": component_type,
                    "start_time": event.start_time,
                    "end_time": event.end_time,
                    "extra_data": extra_data
                }

                result[component_id].append(profile_event)

        return dict(result)

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

        self._check_connected()

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
        self._check_connected()

        node_id_to_address = {}
        for node_info in self.node_table():
            node_id_to_address[node_info["NodeID"]] = "{}:{}".format(
                node_info["NodeManagerAddress"],
                node_info["ObjectManagerPort"])

        all_events = []

        for key, items in self.profile_table().items():
            # Only consider object manager events.
            if items[0]["component_type"] != "object_manager":
                continue

            for event in items:
                if event["event_type"] == "transfer_send":
                    object_id, remote_node_id, _, _ = event["extra_data"]

                elif event["event_type"] == "transfer_receive":
                    object_id, remote_node_id, _, _ = event["extra_data"]

                elif event["event_type"] == "receive_pull_request":
                    object_id, remote_node_id = event["extra_data"]

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
                    "pid": node_id_to_address[key],
                    # The identifier for the row that the event appears in.
                    "tid": node_id_to_address[remote_node_id],
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

    def workers(self):
        """Get a dictionary mapping worker ID to worker information."""
        self._check_connected()

        worker_keys = self.redis_client.keys("Worker*")
        workers_data = {}

        for worker_key in worker_keys:
            worker_info = self.redis_client.hgetall(worker_key)
            worker_id = binary_to_hex(worker_key[len("Workers:"):])

            workers_data[worker_id] = {
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
        if num_tasks == 0:
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
        self._check_connected()

        resources = defaultdict(int)
        clients = self.node_table()
        for client in clients:
            # Only count resources from latest entries of live clients.
            if client["Alive"]:
                for key, value in client["Resources"].items():
                    resources[key] += value
        return dict(resources)

    def _live_client_ids(self):
        """Returns a set of client IDs corresponding to clients still alive."""
        return {
            client["NodeID"]
            for client in self.node_table() if (client["Alive"])
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
        self._check_connected()

        available_resources_by_id = {}

        subscribe_client = self.redis_client.pubsub(
            ignore_subscribe_messages=True)
        subscribe_client.psubscribe(gcs_utils.XRAY_HEARTBEAT_PATTERN)

        client_ids = self._live_client_ids()

        while set(available_resources_by_id.keys()) != client_ids:
            # Parse client message
            raw_message = subscribe_client.get_message()
            if (raw_message is None or raw_message["pattern"] !=
                    gcs_utils.XRAY_HEARTBEAT_PATTERN):
                continue
            data = raw_message["data"]
            pub_message = gcs_utils.PubSubMessage.FromString(data)
            heartbeat_data = pub_message.data
            message = gcs_utils.HeartbeatTableData.FromString(heartbeat_data)
            # Calculate available resources for this client
            num_resources = len(message.resources_available_label)
            dynamic_resources = {}
            for i in range(num_resources):
                resource_id = message.resources_available_label[i]
                dynamic_resources[resource_id] = (
                    message.resources_available_capacity[i])

            # Update available resources for this client
            client_id = ray.utils.binary_to_hex(message.client_id)
            available_resources_by_id[client_id] = dynamic_resources

            # Update clients in cluster
            client_ids = self._live_client_ids()

            # Remove disconnected clients
            for client_id in list(available_resources_by_id.keys()):
                if client_id not in client_ids:
                    del available_resources_by_id[client_id]

        # Calculate total available resources
        total_available_resources = defaultdict(int)
        for available_resources in available_resources_by_id.values():
            for resource_id, num_available in available_resources.items():
                total_available_resources[resource_id] += num_available

        # Close the pubsub clients to avoid leaking file descriptors.
        subscribe_client.close()

        return dict(total_available_resources)

    def _error_messages(self, job_id):
        """Get the error messages for a specific driver.

        Args:
            job_id: The ID of the job to get the errors for.

        Returns:
            A list of the error messages for this driver.
        """
        assert isinstance(job_id, ray.JobID)
        message = self.redis_client.execute_command(
            "RAY.TABLE_LOOKUP", gcs_utils.TablePrefix.Value("ERROR_INFO"), "",
            job_id.binary())

        # If there are no errors, return early.
        if message is None:
            return []

        gcs_entries = gcs_utils.GcsEntry.FromString(message)
        error_messages = []
        for entry in gcs_entries.entries:
            error_data = gcs_utils.ErrorTableData.FromString(entry)
            assert job_id.binary() == error_data.job_id
            error_message = {
                "type": error_data.type,
                "message": error_data.error_message,
                "timestamp": error_data.timestamp,
            }
            error_messages.append(error_message)
        return error_messages

    def error_messages(self, job_id=None):
        """Get the error messages for all drivers or a specific driver.

        Args:
            job_id: The specific job to get the errors for. If this is
                None, then this method retrieves the errors for all jobs.

        Returns:
            A list of the error messages for the specified driver if one was
                given, or a dictionary mapping from job ID to a list of error
                messages for that driver otherwise.
        """
        self._check_connected()

        if job_id is not None:
            assert isinstance(job_id, ray.JobID)
            return self._error_messages(job_id)

        error_table_keys = self.redis_client.keys(
            gcs_utils.TablePrefix_ERROR_INFO_string + "*")
        job_ids = [
            key[len(gcs_utils.TablePrefix_ERROR_INFO_string):]
            for key in error_table_keys
        ]

        return {
            binary_to_hex(job_id): self._error_messages(ray.JobID(job_id))
            for job_id in job_ids
        }

    def actor_checkpoint_info(self, actor_id):
        """Get checkpoint info for the given actor id.
         Args:
            actor_id: Actor's ID.
         Returns:
            A dictionary with information about the actor's checkpoint IDs and
            their timestamps.
        """
        self._check_connected()
        message = self._execute_command(
            actor_id,
            "RAY.TABLE_LOOKUP",
            gcs_utils.TablePrefix.Value("ACTOR_CHECKPOINT_ID"),
            "",
            actor_id.binary(),
        )
        if message is None:
            return None
        gcs_entry = gcs_utils.GcsEntry.FromString(message)
        entry = gcs_utils.ActorCheckpointIdData.FromString(
            gcs_entry.entries[0])
        checkpoint_ids = [
            ray.ActorCheckpointID(checkpoint_id)
            for checkpoint_id in entry.checkpoint_ids
        ]
        return {
            "ActorID": ray.utils.binary_to_hex(entry.actor_id),
            "CheckpointIds": checkpoint_ids,
            "Timestamps": list(entry.timestamps),
        }


state = GlobalState()
"""A global object used to access the cluster's global state."""


def jobs():
    """Get a list of the jobs in the cluster.

    Returns:
        Information from the job table, namely a list of dicts with keys:
        - "JobID" (identifier for the job),
        - "DriverIPAddress" (IP address of the driver for this job),
        - "DriverPid" (process ID of the driver for this job),
        - "StartTime" (UNIX timestamp of the start time of this job),
        - "StopTime" (UNIX timestamp of the stop time of this job, if any)
    """
    return state.job_table()


def nodes():
    """Get a list of the nodes in the cluster.

    Returns:
        Information about the Ray clients in the cluster.
    """
    return state.node_table()


def current_node_id():
    """Return the node id of the current node.

    For example, "node:172.10.5.34". This can be used as a custom resource,
    e.g., {node_id: 1} to reserve the whole node, or {node_id: 0.001} to
    just force placement on the node.

    Returns:
        Id of the current node.
    """
    return ray.resource_spec.NODE_ID_PREFIX + ray.services.get_node_ip_address(
    )


def node_ids():
    """Get a list of the node ids in the cluster.

    For example, ["node:172.10.5.34", "node:172.42.3.77"]. These can be used
    as custom resources, e.g., {node_id: 1} to reserve the whole node, or
    {node_id: 0.001} to just force placement on the node.

    Returns:
        List of the node resource ids.
    """
    node_ids = []
    for node in nodes():
        for k, v in node["Resources"].items():
            if k.startswith(ray.resource_spec.NODE_ID_PREFIX):
                node_ids.append(k)
    return node_ids


def actors(actor_id=None):
    """Fetch and parse the actor info for one or more actor IDs.

    Args:
        actor_id: A hex string of the actor ID to fetch information about. If
            this is None, then all actor information is fetched.

    Returns:
        Information about the actors.
    """
    return state.actor_table(actor_id=actor_id)


def objects(object_id=None):
    """Fetch and parse the object table info for one or more object IDs.

    Args:
        object_id: An object ID to fetch information about. If this is None,
            then the entire object table is fetched.

    Returns:
        Information from the object table.
    """
    return state.object_table(object_id=object_id)


def timeline(filename=None):
    """Return a list of profiling events that can viewed as a timeline.

    To view this information as a timeline, simply dump it as a json file by
    passing in "filename" or using using json.dump, and then load go to
    chrome://tracing in the Chrome web browser and load the dumped file.

    Args:
        filename: If a filename is provided, the timeline is dumped to that
            file.

    Returns:
        If filename is not provided, this returns a list of profiling events.
            Each profile event is a dictionary.
    """
    return state.chrome_tracing_dump(filename=filename)


def object_transfer_timeline(filename=None):
    """Return a list of transfer events that can viewed as a timeline.

    To view this information as a timeline, simply dump it as a json file by
    passing in "filename" or using using json.dump, and then load go to
    chrome://tracing in the Chrome web browser and load the dumped file. Make
    sure to enable "Flow events" in the "View Options" menu.

    Args:
        filename: If a filename is provided, the timeline is dumped to that
            file.

    Returns:
        If filename is not provided, this returns a list of profiling events.
            Each profile event is a dictionary.
    """
    return state.chrome_tracing_object_transfer_dump(filename=filename)


def cluster_resources():
    """Get the current total cluster resources.

    Note that this information can grow stale as nodes are added to or removed
    from the cluster.

    Returns:
        A dictionary mapping resource name to the total quantity of that
            resource in the cluster.
    """
    return state.cluster_resources()


def available_resources():
    """Get the current available cluster resources.

    This is different from `cluster_resources` in that this will return idle
    (available) resources rather than total resources.

    Note that this information can grow stale as tasks start and finish.

    Returns:
        A dictionary mapping resource name to the total quantity of that
            resource in the cluster.
    """
    return state.available_resources()


def errors(all_jobs=False):
    """Get error messages from the cluster.

    Args:
        all_jobs: False if we should only include error messages for this
            specific job, or True if we should include error messages for all
            jobs.

    Returns:
        Error messages pushed from the cluster. This will be a single list if
            all_jobs is False, or a dictionary mapping from job ID to a list of
            error messages for that job if all_jobs is True.
    """
    if not all_jobs:
        worker = ray.worker.global_worker
        error_messages = state.error_messages(job_id=worker.current_job_id)
    else:
        error_messages = state.error_messages(job_id=None)
    return error_messages
