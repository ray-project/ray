import ray
import time
import os
from collections import defaultdict
import aiohttp.web
import asyncio
import socket
import json
from ray.experimental import internal_kv
import ray.dashboard.consts as dashboard_consts


@ray.remote
class _ray_internal_insight_monitor:
    def __init__(self):
        # {job_id: {caller_class.caller_func -> callee_class.callee_func: count}}
        self.call_graph = defaultdict(lambda: defaultdict(int))
        # Maps to track unique actors and methods per job
        self.actors = defaultdict(set)
        self.actor_id_map = defaultdict(dict)  # {job_id: {actor_class: actor_id}}
        self.methods = defaultdict(
            dict
        )  # {job_id: {class.method: {id: unique_id, actorId: actor_id}}}
        self.functions = defaultdict(set)
        self.function_id_map = defaultdict(
            dict
        )  # {job_id: {function_name: function_id}}
        self.actor_counter = defaultdict(int)
        self.method_counter = defaultdict(int)
        self.function_counter = defaultdict(int)

        # Data flow tracking
        self.data_flows = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        self.object_events = defaultdict(lambda: defaultdict())

        # Context info
        self.context_info = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        self.resource_usage = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

        # Start HTTP server
        self.app = aiohttp.web.Application()
        self.app.router.add_get("/get_call_graph_data", self.handle_get_call_graph_data)
        self.app.router.add_get("/get_context_info", self.handle_get_context_info)
        self.app.router.add_get("/get_resource_usage", self.handle_get_resource_usage)
        self.runner = None
        self.site = None
        self.node_ip_address = ray._private.services.get_node_ip_address()
        self.port = self._get_free_port()
        asyncio.create_task(self._start_server())

        # Save address in KV store using _internal_kv_put
        internal_kv._internal_kv_put(
            "insight_monitor_address",
            f"{self.node_ip_address}:{self.port}".encode(),
            namespace="flowinsight",
        )

    def _get_free_port(self):
        """Get a free port on the current node."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port

    async def _start_server(self):
        """Start the HTTP server."""
        self.runner = aiohttp.web.AppRunner(self.app)
        await self.runner.setup()
        self.site = aiohttp.web.TCPSite(self.runner, self.node_ip_address, self.port)
        await self.site.start()
        print(
            f"Insight monitor HTTP server started at http://{self.node_ip_address}:{self.port}"
        )

    async def handle_get_call_graph_data(self, request):
        """Handle HTTP request for call graph data."""
        job_id = request.query.get("job_id", "default_job")
        data = self.get_call_graph_data(job_id)
        return aiohttp.web.json_response(data)

    async def handle_get_context_info(self, request):
        """Handle HTTP request for context info data."""
        job_id = request.query.get("job_id", "default_job")
        data = self.get_context(job_id)
        return aiohttp.web.json_response(data)

    async def handle_get_resource_usage(self, request):
        """Handle HTTP request for resource usage data."""
        job_id = request.query.get("job_id", "default_job")
        data = self.get_resource_usage(job_id)
        return aiohttp.web.json_response(data)

    async def async_emit_call_record(self, call_record):
        self.emit_call_record(call_record)

    def emit_call_record(self, call_record):
        job_id = call_record["job_id"]
        caller_class = call_record["caller_class"]
        caller_func = call_record["caller_func"]
        callee_class = call_record["callee_class"]
        callee_func = call_record["callee_func"]
        call_times = call_record.get("call_times", 1)

        # Create caller and callee identifiers
        caller_id = f"{caller_class}.{caller_func}" if caller_class else caller_func
        callee_id = f"{callee_class}.{callee_func}" if callee_class else callee_func

        # Update call graph
        self.call_graph[job_id][f"{caller_id}->{callee_id}"] += call_times

        # Track actors and methods
        if caller_class:
            self.actors[job_id].add(caller_class)
            if caller_class not in self.actor_id_map[job_id]:
                self.actor_id_map[job_id][caller_class] = caller_class.split(":")[1]

            if caller_id not in self.methods[job_id]:
                self.method_counter[job_id] += 1
                self.methods[job_id][caller_id] = {
                    "id": f"method{self.method_counter[job_id]}",
                    "actorId": self.actor_id_map[job_id][caller_class],
                    "name": caller_func,
                    "class": caller_class,
                }
        else:
            self.functions[job_id].add(caller_func)
            if caller_func not in self.function_id_map[job_id]:
                if caller_func == "main":
                    self.function_id_map[job_id][caller_func] = "main"
                else:
                    self.function_counter[job_id] += 1
                    self.function_id_map[job_id][
                        caller_func
                    ] = f"function{self.function_counter[job_id]}"

        if callee_class:
            self.actors[job_id].add(callee_class)
            if callee_class not in self.actor_id_map[job_id]:
                self.actor_id_map[job_id][callee_class] = callee_class.split(":")[1]

            if callee_id not in self.methods[job_id]:
                self.method_counter[job_id] += 1
                self.methods[job_id][callee_id] = {
                    "id": f"method{self.method_counter[job_id]}",
                    "actorId": self.actor_id_map[job_id][callee_class],
                    "name": callee_func,
                    "class": callee_class,
                }
        else:
            self.functions[job_id].add(callee_func)
            if callee_func not in self.function_id_map[job_id]:
                if callee_func == "main":
                    self.function_id_map[job_id][callee_func] = "main"
                else:
                    self.function_counter[job_id] += 1
                    self.function_id_map[job_id][
                        callee_func
                    ] = f"function{self.function_counter[job_id]}"

    def get_call_graph_data(self, job_id):
        """Return the call graph data for a specific job."""
        graph_data = {
            "actors": [],
            "methods": [],
            "functions": [],
            "callFlows": [],
            "dataFlows": [],
        }

        # Add actors
        for actor_class, actor_id in self.actor_id_map.get(job_id, {}).items():
            graph_data["actors"].append(
                {
                    "id": actor_id,
                    "name": actor_class.split(":")[0],
                    "language": "python",
                }
            )

        # Add methods
        for method_info in self.methods.get(job_id, {}).values():
            graph_data["methods"].append(
                {
                    "id": method_info["id"],
                    "actorId": method_info["actorId"],
                    "name": method_info["name"],
                    "language": "python",
                }
            )

        # Add functions
        for func_name, function_id in self.function_id_map.get(job_id, {}).items():
            if "." not in func_name:  # Ensure it's not a method
                graph_data["functions"].append(
                    {"id": function_id, "name": func_name, "language": "python"}
                )

        # Add call flows
        for call_edge, count in self.call_graph.get(job_id, {}).items():
            caller, callee = call_edge.split("->")

            # Get source ID
            source_id = None
            if caller in self.methods.get(job_id, {}):
                source_id = self.methods[job_id][caller]["id"]
            elif caller in self.function_id_map.get(job_id, {}):
                source_id = self.function_id_map[job_id][caller]

            # Get target ID
            target_id = None
            if callee in self.methods.get(job_id, {}):
                target_id = self.methods[job_id][callee]["id"]
            elif callee in self.function_id_map.get(job_id, {}):
                target_id = self.function_id_map[job_id][callee]

            if source_id and target_id:
                graph_data["callFlows"].append(
                    {"source": source_id, "target": target_id, "count": count}
                )

        # Add data flows with merged statistics
        for flow_key, entry in self.data_flows.get(job_id, {}).items():
            for argpos, flow_stats in entry.items():
                source, target = flow_key.split("->")

                # Get source ID
                source_id = None
                if source in self.methods.get(job_id, {}):
                    source_id = self.methods[job_id][source]["id"]
                elif source in self.function_id_map.get(job_id, {}):
                    source_id = self.function_id_map[job_id][source]

                # Get target ID
                target_id = None
                if target in self.methods.get(job_id, {}):
                    target_id = self.methods[job_id][target]["id"]
                elif target in self.function_id_map.get(job_id, {}):
                    target_id = self.function_id_map[job_id][target]

                if source_id and target_id:
                    total_size_mb = flow_stats["size"] / (1024 * 1024)
                    graph_data["dataFlows"].append(
                        {
                            "argpos": argpos,
                            "source": source_id,
                            "target": target_id,
                            "duration": flow_stats["duration"],
                            "size": total_size_mb,
                            "timestamp": flow_stats["timestamp"],
                        }
                    )

        return graph_data

    async def async_emit_object_record_get(self, recv_record):
        self.emit_object_record_get(recv_record)

    def emit_object_record_get(self, recv_record):
        """Record object transfer between methods/functions."""
        job_id = recv_record["job_id"]
        object_id = recv_record["object_id"]
        timestamp = recv_record["timestamp"]
        object_event = self.object_events.get(job_id, {}).get(object_id, {})
        if len(object_event) == 0:
            return
        caller_class = object_event.get("caller_class", "")
        caller_func = object_event.get("caller_func", "")
        callee_class = recv_record.get("recv_class", "")
        callee_func = recv_record.get("recv_func", "")
        argpos = object_event.get("argpos", 0)
        size = object_event.get("size", 0)

        if object_id in self.object_events.get(job_id, {}):
            del self.object_events[job_id][object_id]

        # Create source and target identifiers
        source = f"{caller_class}.{caller_func}" if caller_class else caller_func
        target = f"{callee_class}.{callee_func}" if callee_class else callee_func

        # Update data flow tracking with accumulated values
        flow_key = f"{source}->{target}"
        duration = timestamp - object_event["timestamp"]
        self.data_flows[job_id][flow_key][argpos]["size"] = size
        self.data_flows[job_id][flow_key][argpos]["duration"] = duration
        self.data_flows[job_id][flow_key][argpos]["timestamp"] = timestamp

    async def async_emit_object_record_put(self, object_record):
        self.emit_object_record_put(object_record)

    def emit_object_record_put(self, object_record):
        """Record object transfer between methods/functions."""
        job_id = object_record["job_id"]
        object_id = object_record["object_id"]
        self.object_events[job_id][object_id] = object_record

    def emit_context(self, context_info):
        """Record context info."""
        job_id = context_info["job_id"]
        actor_id = context_info["actor_id"]
        self.context_info[job_id][actor_id].update(context_info["context"])

    def get_context(self, job_id):
        """Get context info."""
        return self.context_info[job_id]

    async def emit_resource_usage(self, resource_usage):
        """Record resource usage."""
        job_id = resource_usage["job_id"]
        actor_id = resource_usage["actor_id"]
        self.resource_usage[job_id][actor_id].update(resource_usage["usage"])

    def get_resource_usage(self, job_id):
        """Get resource usage."""
        return self.resource_usage[job_id]


_inner_class_name = "_ray_internal_insight_monitor"
_null_object_id = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
monitor_actor = None


def get_monitor_actor():
    global monitor_actor
    if monitor_actor is not None:
        return monitor_actor
    try:
        monitor_actor = ray.get_actor(_inner_class_name, namespace="flowinsight")
    except ValueError:
        monitor_actor = _ray_internal_insight_monitor.options(
            name=_inner_class_name,
            namespace="flowinsight",
            lifetime="detached",
        ).remote()
    return monitor_actor


def _get_current_task_name():
    if ray.get_runtime_context().worker.mode == ray._private.worker.WORKER_MODE:
        current_task_name = ray.get_runtime_context().get_task_name()
        if current_task_name is not None:
            return current_task_name.split(".")[-1]
    return "main"


def _get_caller_class():
    caller_class = None
    try:
        # caller actor can be fetched from the runtime context
        # but it may raise Exception if called in the driver or in a task
        caller_actor = ray.get_runtime_context().current_actor
        if caller_actor is not None:
            caller_class = (
                caller_actor._ray_actor_creation_function_descriptor.class_name
                + ":"
                + caller_actor._ray_actor_id.hex()
            )
    except Exception:
        pass

    return caller_class


def is_flow_insight_enabled():
    """
    Check if flow insight is enabled.
    """
    return os.getenv(dashboard_consts.FLOW_INSIGHT_ENABLED_ENV_VAR, "0") == "1"


def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        # If we have a running loop, use run_coroutine_threadsafe
        asyncio.ensure_future(coro, loop=loop)
    except RuntimeError:
        # If no loop is running, create one and run until complete
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(coro)


def record_control_flow(callee_class, callee_func):
    """
    record the control flow between the caller and the callee
    this will get caller context automatically from the runtime context

    param:
        callee_class: the class name of the callee
        callee_func: the function name of the callee
    """
    if not is_flow_insight_enabled():
        return

    if callee_class is not None and callee_class.startswith(_inner_class_name):
        return

    caller_class = _get_caller_class()
    caller_func = _get_current_task_name()
    # Create a record for this call
    call_record = {
        "caller_class": caller_class,
        "caller_func": caller_func,
        "callee_class": callee_class,
        "callee_func": callee_func,
        "call_times": 1,
        "job_id": ray.get_runtime_context().get_job_id(),
    }
    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_call_record.remote(call_record)

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_call_record.remote(call_record))


def record_object_arg_get(object_id):
    """
    record the object get event for the task's args
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the task's args
    """
    if not is_flow_insight_enabled():
        return

    if object_id is None or object_id == _null_object_id:
        return
    caller_class = _get_caller_class()

    if caller_class is not None and caller_class.startswith(
        "_ray_internal_insight_monitor"
    ):
        return

    recv_func = _get_current_task_name()

    object_recv_record = {
        "object_id": object_id,
        "recv_class": caller_class,
        "recv_func": recv_func,
        "timestamp": time.time(),
        "job_id": ray.get_runtime_context().get_job_id(),
    }
    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_object_record_get.remote(
                object_recv_record
            )

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_object_record_get.remote(object_recv_record))


def record_object_put(object_id, size):
    """
    record the object put event for a general ray.put
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the object to be put
        size: the size of the object to be put
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return

    caller_class = _get_caller_class()
    caller_func = _get_current_task_name()
    # Create a record for this call
    object_record = {
        "object_id": object_id,
        "size": size,
        "argpos": -2,
        "timestamp": time.time(),
        "caller_class": caller_class,
        "caller_func": caller_func,
        "job_id": ray.get_runtime_context().get_job_id(),
    }

    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_object_record_put.remote(object_record)

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_object_record_put.remote(object_record))


def record_object_arg_put(object_id, argpos, size, callee):
    """
    record the object put event for the task's args
    this will get caller context automatically from the runtime context
    callee is used to prevent recursive call for monitor actor

    param:
        object_id: the object id of the task's args
        size: the size of the task's args
        callee: the callee function info, e.g. "ActorClass.method_name"
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return
    callee_class = None
    callee_info = callee.split(".")
    if len(callee_info) == 2:
        callee_class = None
    elif len(callee_info) == 3:
        callee_class = callee_info[-2]

    if callee_class is not None and callee_class.startswith(_inner_class_name):
        return

    caller_class = _get_caller_class()
    caller_func = _get_current_task_name()
    # Create a record for this call
    object_record = {
        "object_id": object_id,
        "argpos": argpos,
        "size": size,
        "timestamp": time.time(),
        "caller_class": caller_class,
        "caller_func": caller_func,
        "job_id": ray.get_runtime_context().get_job_id(),
    }

    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_object_record_put.remote(object_record)

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_object_record_put.remote(object_record))


def record_object_return_put(object_id, size):
    """
    record the object put event for the task's return value
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the task's return value
        size: the size of the task's return value
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return

    if size == 0:
        return

    caller_class = _get_caller_class()

    if caller_class is not None and caller_class.startswith(_inner_class_name):
        return

    # Get the task name from the runtime context
    # if there is no task name, it should be the driver
    caller_func = _get_current_task_name()
    # Create a record for this call
    object_record = {
        "object_id": object_id,
        "size": size,
        "argpos": -1,
        "timestamp": time.time(),
        "caller_class": caller_class,
        "caller_func": caller_func,
        "job_id": ray.get_runtime_context().get_job_id(),
    }

    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_object_record_put.remote(object_record)

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_object_record_put.remote(object_record))


def record_object_get(object_id, task_id):
    """
    record the object get event for a general ray.get
    this will get caller context automatically from the runtime context
    task_id is used to prevent recursive call for monitor actor
    since we can get callee actor id from the task_id

    param:
        object_id: the object id of the object to be get
        task_id: the task id of the task to be get
    """
    if not is_flow_insight_enabled():
        return

    if object_id is None or object_id == _null_object_id:
        return

    # Get the task name from the runtime context
    # if there is no task name, it should be the driver
    recv_func = _get_current_task_name()
    caller_class = _get_caller_class()

    object_recv_record = {
        "object_id": object_id,
        "recv_class": caller_class,
        "recv_func": recv_func,
        "timestamp": time.time(),
        "job_id": ray.get_runtime_context().get_job_id(),
    }

    if task_id.actor_id() == monitor_actor._ray_actor_id:
        return

    if ray._private.worker.global_worker.core_worker.current_actor_is_asyncio():

        async def _emit():
            await get_monitor_actor().async_emit_object_record_get.remote(
                object_recv_record
            )

        run_async(_emit())
    else:
        ray.get(get_monitor_actor().emit_object_record_get.remote(object_recv_record))


def report_resource_usage(usage: dict):
    """
    report the resource usage of the current task
    usage is a dict of the resource usage
    e.g. {"torch_gram": {"used": 1024, "base": "gpu"}}
    """
    if not is_flow_insight_enabled():
        return

    current_class = _get_caller_class()
    if current_class is None:
        return
    actor_info = current_class.split(":")
    ray.get(
        get_monitor_actor().emit_resource_usage.remote(
            {
                "actor_id": actor_info[1],
                "job_id": ray.get_runtime_context().get_job_id(),
                "usage": usage,
            }
        )
    )


async def async_register_current_context(context: dict):
    """
    register the current context info of the current node
    """
    if not is_flow_insight_enabled():
        return

    current_class = _get_caller_class()
    if current_class is None:
        return
    actor_info = current_class.split(":")
    await get_monitor_actor().emit_context.remote(
        {
            "actor_id": actor_info[1],
            "job_id": ray.get_runtime_context().get_job_id(),
            "context": context,
        }
    )


def register_current_context(context: dict):
    """
    register the current context info of the current node
    """
    if not is_flow_insight_enabled():
        return

    current_class = _get_caller_class()
    if current_class is None:
        return
    actor_info = current_class.split(":")
    ray.get(
        get_monitor_actor().emit_context.remote(
            {
                "actor_id": actor_info[1],
                "job_id": ray.get_runtime_context().get_job_id(),
                "context": context,
            }
        )
    )


def report_torch_gram():
    """
    report the torch gram usage of the current task
    """
    if not is_flow_insight_enabled():
        return

    try:
        import torch
    except ImportError:
        return

    report_resource_usage(
        {
            "torch_gram_allocated": {
                "used": torch.cuda.memory_allocated() / 1024 / 1024,
                "base": "gpu",
            },
            "torch_gram_max_allocated": {
                "used": torch.cuda.max_memory_allocated() / 1024 / 1024,
                "base": "gpu",
            },
        }
    )


async def async_report_resource_usage(usage: dict):
    """
    report the resource usage of the current task
    usage is a dict of the resource usage
    e.g. {"torch_gram": {"used": 1024, "base": "gpu"}}
    """
    if not is_flow_insight_enabled():
        return

    current_class = _get_caller_class()
    if current_class is None:
        return
    actor_info = current_class.split(":")
    await get_monitor_actor().emit_resource_usage.remote(
        {
            "actor_id": actor_info[1],
            "job_id": ray.get_runtime_context().get_job_id(),
            "usage": usage,
        }
    )


async def async_report_torch_gram():
    """
    report the torch gram usage of the current task
    """
    if not is_flow_insight_enabled():
        return

    try:
        import torch
    except ImportError:
        return

    await async_report_resource_usage(
        {
            "torch_gram_allocated": {
                "used": torch.cuda.memory_allocated() / 1024 / 1024,
                "base": "gpu",
            },
            "torch_gram_max_allocated": {
                "used": torch.cuda.max_memory_allocated() / 1024 / 1024,
                "base": "gpu",
            },
        }
    )
