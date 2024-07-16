import json
import logging
from collections import defaultdict
from typing import Dict

from ray._private.protobuf_compat import message_to_dict

import ray
from ray._private.client_mode_hook import client_mode_hook
from ray._private.resource_spec import NODE_ID_PREFIX, HEAD_NODE_RESOURCE_NAME
from ray._private.utils import (
    binary_to_hex,
    decode,
    hex_to_binary,
    validate_actor_state_name,
)
from ray._raylet import GlobalStateAccessor
from ray.core.generated import common_pb2
from ray.core.generated import gcs_pb2
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


class GlobalState:
    """A class used to interface with the Ray control state.

    Attributes:
        global_state_accessor: The client used to query gcs table from gcs
            server.
    """

    def __init__(self):
        """Create a GlobalState object."""
        # Args used for lazy init of this object.
        self.gcs_options = None
        self.global_state_accessor = None

    def _check_connected(self):
        """Ensure that the object has been initialized before it is used.

        This lazily initializes clients needed for state accessors.

        Raises:
            RuntimeError: An exception is raised if ray.init() has not been
                called yet.
        """
        if self.gcs_options is not None and self.global_state_accessor is None:
            self._really_init_global_state()

        # _really_init_global_state should have set self.global_state_accessor
        if self.global_state_accessor is None:
            raise ray.exceptions.RaySystemError(
                "Ray has not been started yet. You can start Ray with 'ray.init()'."
            )

    def disconnect(self):
        """Disconnect global state from GCS."""
        self.gcs_options = None
        if self.global_state_accessor is not None:
            self.global_state_accessor.disconnect()
            self.global_state_accessor = None

    def _initialize_global_state(self, gcs_options):
        """Set args for lazily initialization of the GlobalState object.

        It's possible that certain keys in gcs kv may not have been fully
        populated yet. In this case, we will retry this method until they have
        been populated or we exceed a timeout.

        Args:
            gcs_options: The client options for gcs
        """

        # Save args for lazy init of global state. This avoids opening extra
        # gcs connections from each worker until needed.
        self.gcs_options = gcs_options

    def _really_init_global_state(self):
        self.global_state_accessor = GlobalStateAccessor(self.gcs_options)
        self.global_state_accessor.connect()

    def actor_table(
        self, actor_id: str, job_id: ray.JobID = None, actor_state_name: str = None
    ):
        """Fetch and parse the actor table information for a single actor ID.

        Args:
            actor_id: A hex string of the actor ID to fetch information about.
                If this is None, then the actor table is fetched.
                If this is not None, `job_id` and `actor_state_name`
                will not take effect.
            job_id: To filter actors by job_id, which is of type `ray.JobID`.
                You can use the `ray.get_runtime_context().job_id` function
                to get the current job ID
            actor_state_name: To filter actors based on actor state,
                which can be one of the following: "DEPENDENCIES_UNREADY",
                "PENDING_CREATION", "ALIVE", "RESTARTING", or "DEAD".
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
                actor_table_data = gcs_pb2.ActorTableData.FromString(actor_info)
                return self._gen_actor_info(actor_table_data)
        else:
            validate_actor_state_name(actor_state_name)
            actor_table = self.global_state_accessor.get_actor_table(
                job_id, actor_state_name
            )
            results = {}
            for i in range(len(actor_table)):
                actor_table_data = gcs_pb2.ActorTableData.FromString(actor_table[i])
                results[
                    binary_to_hex(actor_table_data.actor_id)
                ] = self._gen_actor_info(actor_table_data)

            return results

    def _gen_actor_info(self, actor_table_data):
        """Parse actor table data.

        Returns:
            Information from actor table.
        """
        actor_info = {
            "ActorID": binary_to_hex(actor_table_data.actor_id),
            "ActorClassName": actor_table_data.class_name,
            "IsDetached": actor_table_data.is_detached,
            "Name": actor_table_data.name,
            "JobID": binary_to_hex(actor_table_data.job_id),
            "Address": {
                "IPAddress": actor_table_data.address.ip_address,
                "Port": actor_table_data.address.port,
                "NodeID": binary_to_hex(actor_table_data.address.raylet_id),
            },
            "OwnerAddress": {
                "IPAddress": actor_table_data.owner_address.ip_address,
                "Port": actor_table_data.owner_address.port,
                "NodeID": binary_to_hex(actor_table_data.owner_address.raylet_id),
            },
            "State": gcs_pb2.ActorTableData.ActorState.DESCRIPTOR.values_by_number[
                actor_table_data.state
            ].name,
            "NumRestarts": actor_table_data.num_restarts,
            "Timestamp": actor_table_data.timestamp,
            "StartTime": actor_table_data.start_time,
            "EndTime": actor_table_data.end_time,
            "DeathCause": actor_table_data.death_cause,
            "Pid": actor_table_data.pid,
        }
        return actor_info

    def node_table(self):
        """Fetch and parse the Gcs node info table.

        Returns:
            Information about the node in the cluster.
        """
        self._check_connected()

        return self.global_state_accessor.get_node_table()

    def job_table(self):
        """Fetch and parse the gcs job table.

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
            entry = gcs_pb2.JobTableData.FromString(job_table[i])
            job_info = {}
            job_info["JobID"] = entry.job_id.hex()
            job_info["DriverIPAddress"] = entry.driver_address.ip_address
            job_info["DriverPid"] = entry.driver_pid
            job_info["Timestamp"] = entry.timestamp
            job_info["StartTime"] = entry.start_time
            job_info["EndTime"] = entry.end_time
            job_info["IsDead"] = entry.is_dead
            job_info["Entrypoint"] = entry.entrypoint
            results.append(job_info)

        return results

    def next_job_id(self):
        """Get next job id from GCS.

        Returns:
            Next job id in the cluster.
        """
        self._check_connected()

        return ray.JobID.from_int(self.global_state_accessor.get_next_job_id())

    def profile_events(self):
        """Retrieve and return task profiling events from GCS.

        Return:
            Profiling events by component id (e.g. worker id).
            {
                <component_id>: [
                    {
                        event_type: <event name> ,
                        component_id: <i.e. worker id>,
                        node_ip_address: <on which node profiling was done>,
                        component_type: <i.e. worker/driver>,
                        start_time: <unix timestamp in seconds>,
                        end_time: <unix timestamp in seconds>,
                        extra_data: <e.g. stack trace when error raised>,
                    }
                ]
            }
        """
        self._check_connected()

        result = defaultdict(list)
        task_events = self.global_state_accessor.get_task_events()
        for i in range(len(task_events)):
            event = gcs_pb2.TaskEvents.FromString(task_events[i])
            profile = event.profile_events
            if not profile:
                continue

            component_type = profile.component_type
            component_id = binary_to_hex(profile.component_id)
            node_ip_address = profile.node_ip_address

            for event in profile.events:
                try:
                    extra_data = json.loads(event.extra_data)
                except ValueError:
                    extra_data = {}
                profile_event = {
                    "event_type": event.event_name,
                    "component_id": component_id,
                    "node_ip_address": node_ip_address,
                    "component_type": component_type,
                    "start_time": event.start_time,
                    "end_time": event.end_time,
                    "extra_data": extra_data,
                }

                result[component_id].append(profile_event)

        return dict(result)

    def get_placement_group_by_name(self, placement_group_name, ray_namespace):
        self._check_connected()

        placement_group_info = self.global_state_accessor.get_placement_group_by_name(
            placement_group_name, ray_namespace
        )
        if placement_group_info is None:
            return None
        else:
            placement_group_table_data = gcs_pb2.PlacementGroupTableData.FromString(
                placement_group_info
            )
            return self._gen_placement_group_info(placement_group_table_data)

    def placement_group_table(self, placement_group_id=None):
        self._check_connected()

        if placement_group_id is not None:
            placement_group_id = ray.PlacementGroupID(
                hex_to_binary(placement_group_id.hex())
            )
            placement_group_info = self.global_state_accessor.get_placement_group_info(
                placement_group_id
            )
            if placement_group_info is None:
                return {}
            else:
                placement_group_info = gcs_pb2.PlacementGroupTableData.FromString(
                    placement_group_info
                )
                return self._gen_placement_group_info(placement_group_info)
        else:
            placement_group_table = (
                self.global_state_accessor.get_placement_group_table()
            )
            results = {}
            for placement_group_info in placement_group_table:
                placement_group_table_data = gcs_pb2.PlacementGroupTableData.FromString(
                    placement_group_info
                )
                placement_group_id = binary_to_hex(
                    placement_group_table_data.placement_group_id
                )
                results[placement_group_id] = self._gen_placement_group_info(
                    placement_group_table_data
                )

            return results

    def _gen_placement_group_info(self, placement_group_info):
        # This should be imported here, otherwise, it will error doc build.
        from ray.core.generated.common_pb2 import PlacementStrategy

        def get_state(state):
            if state == gcs_pb2.PlacementGroupTableData.PENDING:
                return "PENDING"
            elif state == gcs_pb2.PlacementGroupTableData.CREATED:
                return "CREATED"
            elif state == gcs_pb2.PlacementGroupTableData.RESCHEDULING:
                return "RESCHEDULING"
            else:
                return "REMOVED"

        def get_strategy(strategy):
            if strategy == PlacementStrategy.PACK:
                return "PACK"
            elif strategy == PlacementStrategy.STRICT_PACK:
                return "STRICT_PACK"
            elif strategy == PlacementStrategy.STRICT_SPREAD:
                return "STRICT_SPREAD"
            elif strategy == PlacementStrategy.SPREAD:
                return "SPREAD"
            else:
                raise ValueError(f"Invalid strategy returned: {PlacementStrategy}")

        stats = placement_group_info.stats
        assert placement_group_info is not None
        return {
            "placement_group_id": binary_to_hex(
                placement_group_info.placement_group_id
            ),
            "name": placement_group_info.name,
            "bundles": {
                # The value here is needs to be dictionarified
                # otherwise, the payload becomes unserializable.
                bundle.bundle_id.bundle_index: message_to_dict(bundle)["unitResources"]
                for bundle in placement_group_info.bundles
            },
            "bundles_to_node_id": {
                bundle.bundle_id.bundle_index: binary_to_hex(bundle.node_id)
                for bundle in placement_group_info.bundles
            },
            "strategy": get_strategy(placement_group_info.strategy),
            "state": get_state(placement_group_info.state),
            "stats": {
                "end_to_end_creation_latency_ms": (
                    stats.end_to_end_creation_latency_us / 1000.0
                ),
                "scheduling_latency_ms": (stats.scheduling_latency_us / 1000.0),
                "scheduling_attempt": stats.scheduling_attempt,
                "highest_retry_delay_ms": stats.highest_retry_delay_ms,
                "scheduling_state": gcs_pb2.PlacementGroupStats.SchedulingState.DESCRIPTOR.values_by_number[  # noqa: E501
                    stats.scheduling_state
                ].name,
            },
        }

    def _nanoseconds_to_microseconds(self, time_in_nanoseconds):
        """A helper function for converting nanoseconds to microseconds."""
        time_in_microseconds = time_in_nanoseconds / 1000
        return time_in_microseconds

    # Colors are specified at
    # https://github.com/catapult-project/catapult/blob/master/tracing/tracing/base/color_scheme.html.  # noqa: E501
    _default_color_mapping = defaultdict(
        lambda: "generic_work",
        {
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
        },
    )

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

        # Add a small delay to account for propagation delay of events to the GCS.
        # This should be harmless enough but prevents calls to timeline() from
        # missing recent timeline data.
        import time

        time.sleep(1)

        profile_events = self.profile_events()
        all_events = []

        for component_id_hex, component_events in profile_events.items():
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
                    "tid": event["component_type"] + ":" + event["component_id"],
                    # The start time in microseconds.
                    "ts": self._nanoseconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._nanoseconds_to_microseconds(
                        event["end_time"] - event["start_time"]
                    ),
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

        if not all_events:
            logger.warning(
                "No profiling events found. Ray profiling must be enabled "
                "by setting RAY_PROFILING=1, and make sure "
                "RAY_task_events_report_interval_ms=0."
            )

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
                node_info["NodeManagerAddress"], node_info["ObjectManagerPort"]
            )

        all_events = []

        for key, items in self.profile_events().items():
            # Only consider object manager events.
            if items[0]["component_type"] != "object_manager":
                continue

            for event in items:
                if event["event_type"] == "transfer_send":
                    object_ref, remote_node_id, _, _ = event["extra_data"]

                elif event["event_type"] == "transfer_receive":
                    object_ref, remote_node_id, _ = event["extra_data"]

                elif event["event_type"] == "receive_pull_request":
                    object_ref, remote_node_id = event["extra_data"]

                else:
                    assert False, "This should be unreachable."

                # Choose a color by reading the first couple of hex digits of
                # the object ref as an integer and turning that into a color.
                object_ref_int = int(object_ref[:2], 16)
                color = self._chrome_tracing_colors[
                    object_ref_int % len(self._chrome_tracing_colors)
                ]

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
                    "ts": self._nanoseconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._nanoseconds_to_microseconds(
                        event["end_time"] - event["start_time"]
                    ),
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

        # Get all data in worker table
        worker_table = self.global_state_accessor.get_worker_table()
        workers_data = {}
        for i in range(len(worker_table)):
            worker_table_data = gcs_pb2.WorkerTableData.FromString(worker_table[i])
            if (
                worker_table_data.is_alive
                and worker_table_data.worker_type == common_pb2.WORKER
            ):
                worker_id = binary_to_hex(worker_table_data.worker_address.worker_id)
                worker_info = worker_table_data.worker_info

                workers_data[worker_id] = {
                    "node_ip_address": decode(worker_info[b"node_ip_address"]),
                    "plasma_store_socket": decode(worker_info[b"plasma_store_socket"]),
                }
                if b"stderr_file" in worker_info:
                    workers_data[worker_id]["stderr_file"] = decode(
                        worker_info[b"stderr_file"]
                    )
                if b"stdout_file" in worker_info:
                    workers_data[worker_id]["stdout_file"] = decode(
                        worker_info[b"stdout_file"]
                    )
        return workers_data

    def add_worker(self, worker_id, worker_type, worker_info):
        """Add a worker to the cluster.

        Args:
            worker_id: ID of this worker. Type is bytes.
            worker_type: Type of this worker. Value is common_pb2.DRIVER or
                common_pb2.WORKER.
            worker_info: Info of this worker. Type is dict{str: str}.

        Returns:
             Is operation success
        """
        worker_data = gcs_pb2.WorkerTableData()
        worker_data.is_alive = True
        worker_data.worker_address.worker_id = worker_id
        worker_data.worker_type = worker_type
        for k, v in worker_info.items():
            worker_data.worker_info[k] = bytes(v, encoding="utf-8")
        return self.global_state_accessor.add_worker_info(
            worker_data.SerializeToString()
        )

    def update_worker_debugger_port(self, worker_id, debugger_port):
        """Update the debugger port of a worker.

        Args:
            worker_id: ID of this worker. Type is bytes.
            debugger_port: Port of the debugger. Type is int.

        Returns:
             Is operation success
        """
        self._check_connected()

        assert worker_id is not None, "worker_id is not valid"
        assert (
            debugger_port is not None and debugger_port > 0
        ), "debugger_port is not valid"

        return self.global_state_accessor.update_worker_debugger_port(
            worker_id, debugger_port
        )

    def get_worker_debugger_port(self, worker_id):
        """Get the debugger port of a worker.

        Args:
            worker_id: ID of this worker. Type is bytes.

        Returns:
             Debugger port of the worker.
        """
        self._check_connected()

        assert worker_id is not None, "worker_id is not valid"

        return self.global_state_accessor.get_worker_debugger_port(worker_id)

    def update_worker_num_paused_threads(self, worker_id, num_paused_threads_delta):
        """Updates the number of paused threads of a worker.

        Args:
            worker_id: ID of this worker. Type is bytes.
            num_paused_threads_delta: The delta of the number of paused threads.

        Returns:
             Is operation success
        """
        self._check_connected()

        assert worker_id is not None, "worker_id is not valid"
        assert num_paused_threads_delta is not None, "worker_id is not valid"

        return self.global_state_accessor.update_worker_num_paused_threads(
            worker_id, num_paused_threads_delta
        )

    def cluster_resources(self):
        """Get the current total cluster resources.

        Note that this information can grow stale as nodes are added to or
        removed from the cluster.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        self._check_connected()

        # Calculate total resources.
        total_resources = defaultdict(int)
        for node_total_resources in self.total_resources_per_node().values():
            for resource_id, value in node_total_resources.items():
                total_resources[resource_id] += value

        return dict(total_resources)

    def _live_node_ids(self):
        """Returns a set of node IDs corresponding to nodes still alive."""
        return set(self.total_resources_per_node().keys())

    def available_resources_per_node(self):
        """Returns a dictionary mapping node id to avaiable resources."""
        self._check_connected()
        available_resources_by_id = {}

        all_available_resources = (
            self.global_state_accessor.get_all_available_resources()
        )
        for available_resource in all_available_resources:
            message = gcs_pb2.AvailableResources.FromString(available_resource)
            # Calculate available resources for this node.
            dynamic_resources = {}
            for resource_id, capacity in message.resources_available.items():
                dynamic_resources[resource_id] = capacity
            # Update available resources for this node.
            node_id = ray._private.utils.binary_to_hex(message.node_id)
            available_resources_by_id[node_id] = dynamic_resources

        return available_resources_by_id

    # returns a dict that maps node_id(hex string) to a dict of {resource_id: capacity}
    def total_resources_per_node(self) -> Dict[str, Dict[str, int]]:
        self._check_connected()
        total_resources_by_node = {}

        all_total_resources = self.global_state_accessor.get_all_total_resources()
        for node_total_resources in all_total_resources:
            message = gcs_pb2.TotalResources.FromString(node_total_resources)
            # Calculate total resources for this node.
            node_resources = {}
            for resource_id, capacity in message.resources_total.items():
                node_resources[resource_id] = capacity
            # Update total resources for this node.
            node_id = ray._private.utils.binary_to_hex(message.node_id)
            total_resources_by_node[node_id] = node_resources

        return total_resources_by_node

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

        available_resources_by_id = self.available_resources_per_node()

        # Calculate total available resources.
        total_available_resources = defaultdict(int)
        for available_resources in available_resources_by_id.values():
            for resource_id, num_available in available_resources.items():
                total_available_resources[resource_id] += num_available

        return dict(total_available_resources)

    def get_system_config(self):
        """Get the system config of the cluster."""
        self._check_connected()
        return json.loads(self.global_state_accessor.get_system_config())

    def get_node_to_connect_for_driver(self, node_ip_address):
        """Get the node to connect for a Ray driver."""
        self._check_connected()
        return self.global_state_accessor.get_node_to_connect_for_driver(
            node_ip_address
        )

    def get_node(self, node_id: str):
        """Get the node information for a node id."""
        self._check_connected()
        return self.global_state_accessor.get_node(node_id)

    def get_draining_nodes(self) -> Dict[str, int]:
        """Get all the hex ids of nodes that are being drained
        and the corresponding draining deadline timestamps in ms.

        There is no deadline if the timestamp is 0.
        """
        self._check_connected()
        return self.global_state_accessor.get_draining_nodes()


state = GlobalState()
"""A global object used to access the cluster's global state."""


def jobs():
    """Get a list of the jobs in the cluster (for debugging only).

    Returns:
        Information from the job table, namely a list of dicts with keys:
        - "JobID" (identifier for the job),
        - "DriverIPAddress" (IP address of the driver for this job),
        - "DriverPid" (process ID of the driver for this job),
        - "StartTime" (UNIX timestamp of the start time of this job),
        - "StopTime" (UNIX timestamp of the stop time of this job, if any)
    """
    return state.job_table()


def next_job_id():
    """Get next job id from GCS.

    Returns:
        Next job id in integer representation in the cluster.
    """
    return state.next_job_id()


@DeveloperAPI
@client_mode_hook
def nodes():
    """Get a list of the nodes in the cluster (for debugging only).

    Returns:
        Information about the Ray clients in the cluster.
    """
    return state.node_table()


def workers():
    """Get a list of the workers in the cluster.

    Returns:
        Information about the Ray workers in the cluster.
    """
    return state.workers()


def current_node_id():
    """Return the node id of the current node.

    For example, "node:172.10.5.34". This can be used as a custom resource,
    e.g., {node_id: 1} to reserve the whole node, or {node_id: 0.001} to
    just force placement on the node.

    Returns:
        Id of the current node.
    """
    return NODE_ID_PREFIX + ray.util.get_node_ip_address()


def node_ids():
    """Get a list of the node ids in the cluster.

    For example, ["node:172.10.5.34", "node:172.42.3.77"]. These can be used
    as custom resources, e.g., {node_id: 1} to reserve the whole node, or
    {node_id: 0.001} to just force placement on the node.

    Returns:
        List of the node resource ids.
    """
    node_ids = []
    for node_total_resources in state.total_resources_per_node().values():
        for resource_id in node_total_resources.keys():
            if (
                resource_id.startswith(NODE_ID_PREFIX)
                and resource_id != HEAD_NODE_RESOURCE_NAME
            ):
                node_ids.append(resource_id)
    return node_ids


def actors(
    actor_id: str = None, job_id: ray.JobID = None, actor_state_name: str = None
):
    """Fetch actor info for one or more actor IDs (for debugging only).

    Args:
        actor_id: A hex string of the actor ID to fetch information about. If
            this is None, then all actor information is fetched.
            If this is not None, `job_id` and `actor_state_name`
            will not take effect.
        job_id: To filter actors by job_id, which is of type `ray.JobID`.
            You can use the `ray.get_runtime_context().job_id` function
            to get the current job ID
        actor_state_name: To filter actors based on actor state,
            which can be one of the following: "DEPENDENCIES_UNREADY",
            "PENDING_CREATION", "ALIVE", "RESTARTING", or "DEAD".
    Returns:
        Information about the actors.
    """
    return state.actor_table(
        actor_id=actor_id, job_id=job_id, actor_state_name=actor_state_name
    )


@DeveloperAPI
@client_mode_hook
def timeline(filename=None):
    """Return a list of profiling events that can viewed as a timeline.

    Ray profiling must be enabled by setting the RAY_PROFILING=1 environment
    variable prior to starting Ray, and set RAY_task_events_report_interval_ms=0

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


@DeveloperAPI
@client_mode_hook
def cluster_resources():
    """Get the current total cluster resources.

    Note that this information can grow stale as nodes are added to or removed
    from the cluster.

    Returns:
        A dictionary mapping resource name to the total quantity of that
            resource in the cluster.
    """
    return state.cluster_resources()


@DeveloperAPI
@client_mode_hook
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


@DeveloperAPI
def available_resources_per_node():
    """Get the current available resources of each live node.

    Note that this information can grow stale as tasks start and finish.

    Returns:
        A dictionary mapping node hex id to available resources dictionary.
    """

    return state.available_resources_per_node()


@DeveloperAPI
def total_resources_per_node():
    """Get the current total resources of each live node.

    Note that this information can grow stale as tasks start and finish.

    Returns:
        A dictionary mapping node hex id to total resources dictionary.
    """

    return state.total_resources_per_node()


def update_worker_debugger_port(worker_id, debugger_port):
    """Update the debugger port of a worker.

    Args:
        worker_id: ID of this worker. Type is bytes.
        debugger_port: Port of the debugger. Type is int.

    Returns:
         Is operation success
    """
    return state.update_worker_debugger_port(worker_id, debugger_port)


def update_worker_num_paused_threads(worker_id, num_paused_threads_delta):
    """Update the number of paused threads of a worker.

    Args:
        worker_id: ID of this worker. Type is bytes.
        num_paused_threads_delta: The delta of the number of paused threads.

    Returns:
         Is operation success
    """
    return state.update_worker_num_paused_threads(worker_id, num_paused_threads_delta)


def get_worker_debugger_port(worker_id):
    """Get the debugger port of a worker.

    Args:
        worker_id: ID of this worker. Type is bytes.

    Returns:
         Debugger port of the worker.
    """
    return state.get_worker_debugger_port(worker_id)
