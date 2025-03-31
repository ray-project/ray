import ray
import requests
import time
import os
from collections import defaultdict
import aiohttp.web
import asyncio
import socket
from contextlib import contextmanager
from ray.experimental import internal_kv
import ray.dashboard.consts as dashboard_consts
from collections import deque
import json

insight_monitor_address = None


def _get_insight_monitor_address():
    """Get the insight monitor address from internal_kv."""
    if not is_flow_insight_enabled():
        return None
    global insight_monitor_address
    if insight_monitor_address is not None:
        return insight_monitor_address
    while True:
        try:
            insight_monitor_address = internal_kv._internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
            ).decode()
            return insight_monitor_address
        except Exception:
            time.sleep(1)


def get_current_worker_id():
    """
    Get the current worker ID.
    """
    return ray._private.worker.global_worker.worker_id


def get_current_job_id():
    """
    Get the current job ID.
    """
    return ray._private.worker.global_worker.current_job_id.hex()


def create_insight_monitor_actor():
    if not is_flow_insight_enabled():
        return
    try:
        ray.get_actor("_ray_internal_insight_monitor", namespace="flowinsight")
    except ValueError:
        _ray_internal_insight_monitor.options(
            name="_ray_internal_insight_monitor",
            namespace="flowinsight",
            lifetime="detached",
        ).remote()


def emit_request(endpoint, payload):
    url = f"http://{_get_insight_monitor_address()}/{endpoint}"
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, data=data, headers=headers, timeout=300)
        if response.status_code != 200:
            print(
                f"Error sending HTTP request: {response.status_code} {response.reason}"
            )
    except Exception as e:
        print(f"Error sending HTTP request: {e}")


@ray.remote
class _ray_internal_insight_monitor:
    def __init__(self):
        # {job_id: {caller_class.caller_func -> callee_class.callee_func: count}}
        self.call_graph = defaultdict(
            lambda: defaultdict(lambda: {"count": 0, "start_time": 0})
        )
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
        self.flow_record = defaultdict(list)
        self.start_time_record = defaultdict(lambda: defaultdict(dict))

        # Data flow tracking
        self.data_flows = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        self.object_events = defaultdict(lambda: defaultdict())
        self.caller_info = defaultdict(lambda: defaultdict(list))

        # Context info
        self.context_info = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        self.resource_usage = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

        # {job_id: {caller_class.caller_func: {total_time, call_count, children: {callee: time}}}}
        self.flame_graph_aggregated = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "actor_name": "",
                    "total_time": 0,
                    "call_count": 0,
                    "durations": defaultdict(float),
                    "total_in_parent": defaultdict(float),
                }
            )
        )

        # Start HTTP server
        self.app = aiohttp.web.Application()
        self.app.router.add_get("/get_call_graph_data", self.handle_get_call_graph_data)
        self.app.router.add_get("/get_context_info", self.handle_get_context_info)
        self.app.router.add_get("/get_resource_usage", self.handle_get_resource_usage)
        self.app.router.add_get(
            "/get_flame_graph_data", self.handle_get_flame_graph_data
        )

        # Add new HTTP handlers for the data collection endpoints
        self.app.router.add_post("/emit-call-record", self.handle_emit_call_record)
        self.app.router.add_post(
            "/emit-object-record-get", self.handle_emit_object_record_get
        )
        self.app.router.add_post(
            "/emit-object-record-put", self.handle_emit_object_record_put
        )
        self.app.router.add_post("/emit-context", self.handle_emit_context)
        self.app.router.add_post(
            "/emit-resource-usage", self.handle_emit_resource_usage
        )
        self.app.router.add_post("/emit-task-end", self.handle_emit_task_end)
        self.app.router.add_post("/emit-caller-info", self.handle_emit_caller_info)

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
        stack_mode = request.query.get("stack_mode", "0")
        data = self.get_call_graph_data(job_id, stack_mode)
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

    async def handle_get_flame_graph_data(self, request):
        """Handle HTTP request for flame graph data."""
        job_id = request.query.get("job_id", "default_job")
        data = self.get_flame_graph_data(job_id)
        return aiohttp.web.json_response(data)

    def emit_call_record(self, call_record):
        job_id = call_record["job_id"]
        caller_class = call_record["caller_class"]
        caller_func = call_record["caller_func"]
        callee_class = call_record["callee_class"]
        callee_func = call_record["callee_func"]
        call_times = call_record.get("call_times", 1)
        # Create caller and callee identifiers for parent-child relationship
        caller_id = f"{caller_class}.{caller_func}" if caller_class else caller_func
        callee_id = f"{callee_class}.{callee_func}" if callee_class else callee_func
        current_task_id = call_record["current_task_id"]
        start_time = call_record["start_time"]
        if caller_id not in self.start_time_record[job_id][callee_id]:
            self.start_time_record[job_id][callee_id][caller_id] = start_time

        self.flow_record[job_id].append(
            {
                "type": "enter",
                "caller_id": caller_id,
                "callee_id": callee_id,
                "caller_task_id": current_task_id,
            }
        )

        # Update call graph
        self.call_graph[job_id][f"{caller_id}->{callee_id}"]["count"] += call_times
        self.call_graph[job_id][f"{caller_id}->{callee_id}"]["start_time"] = start_time

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
                if caller_func == "_main":
                    self.function_id_map[job_id][caller_func] = "_main"
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
                if callee_func == "_main":
                    self.function_id_map[job_id][callee_func] = "_main"
                else:
                    self.function_counter[job_id] += 1
                    self.function_id_map[job_id][
                        callee_func
                    ] = f"function{self.function_counter[job_id]}"

    def get_call_graph_data(self, job_id, stack_mode="0"):
        """Return the call graph data for a specific job."""
        graph_data = {
            "actors": [],
            "methods": [],
            "functions": [],
            "callFlows": [],
            "dataFlows": [],
        }

        call_graph = self.call_graph[job_id]
        if stack_mode == "1":
            (
                call_graph,
                reachable_methods,
                reachable_actors,
                reachable_funcs,
            ) = self.filter_call_graph_data(job_id, self.call_graph[job_id])
        # Add actors
        for actor_class, actor_id in self.actor_id_map.get(job_id, {}).items():
            if stack_mode == "1" and actor_class not in reachable_actors:
                continue
            graph_data["actors"].append(
                {
                    "id": actor_id,
                    "name": actor_class.split(":")[0],
                    "language": "python",
                }
            )

        # Add methods
        for method_info in self.methods.get(job_id, {}).values():
            if stack_mode == "1":
                if (
                    method_info["actorId"] + "." + method_info["name"]
                    not in reachable_methods
                ):
                    continue
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
                if stack_mode == "1" and func_name not in reachable_funcs:
                    continue
                graph_data["functions"].append(
                    {"id": function_id, "name": func_name, "language": "python"}
                )

        # Add call flows
        for call_edge, info in call_graph.items():
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
                    {
                        "source": source_id,
                        "target": target_id,
                        "count": info["count"],
                        "start_time": info["start_time"],
                    }
                )

        # Add data flows with merged statistics
        for flow_key, entry in self.data_flows.get(job_id, {}).items():
            for argpos, flow_stats in entry.items():
                source, target = flow_key.split("->")
                if stack_mode == "1":
                    if "." in source:
                        actor_class = source.split(".")[0]
                        if source.split(":")[1] not in reachable_methods:
                            continue
                        if actor_class not in reachable_actors:
                            continue
                    else:
                        if source not in reachable_funcs:
                            continue
                    if "." in target:
                        actor_class = target.split(".")[0]
                        if target.split(":")[1] not in reachable_methods:
                            continue
                        if actor_class not in reachable_actors:
                            continue
                    else:
                        if target not in reachable_funcs:
                            continue

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
        if len(graph_data["functions"]) == 0:
            graph_data["functions"].append(
                {
                    "id": "_main",
                    "name": "_main",
                    "language": "python",
                }
            )

        return graph_data

    def filter_call_graph_data(self, job_id, call_graph):
        """Filter the call graph data to keep only edges that are part of paths leading to target edges.

        A target edge is defined as caller_id->callee_id where caller_id and callee_id come from
        the flow_record and caller_info mappings. The algorithm uses reverse graph traversal
        to efficiently identify all edges that can reach these target edges.
        """
        # First identify all target edges we want to reach
        target_edges = {}
        reachable_nodes = set()
        reachable_methods = set()
        reachable_actors = set()
        reachable_funcs = set()

        # Build target edges from flow records
        for flow_record in self.flow_record[job_id]:
            if flow_record["type"] == "enter":
                target_edges[flow_record["caller_task_id"]] = (
                    flow_record["caller_id"],
                    flow_record["callee_id"],
                )
                reachable_nodes.add(flow_record["caller_id"])
                reachable_nodes.add(flow_record["callee_id"])
            if flow_record["type"] == "exit":
                caller_infos = self.caller_info[job_id][flow_record["callee_task_id"]]
                for caller_info in caller_infos:
                    caller_task_id = caller_info["task_id"]
                    caller_id = (
                        f"{caller_info['class']}.{caller_info['func']}"
                        if caller_info["class"]
                        else caller_info["func"]
                    )
                    callee_id = flow_record["callee_id"]
                    if caller_task_id in target_edges:
                        del target_edges[caller_task_id]
                    if caller_id in reachable_nodes:
                        reachable_nodes.remove(caller_id)
                    if callee_id in reachable_nodes:
                        reachable_nodes.remove(callee_id)

        target_edges = set(target_edges.values())

        # Build reverse adjacency list for efficient backwards traversal
        reverse_adj = defaultdict(set)
        for edge, _ in call_graph.items():
            src, dst = edge.split("->")
            reverse_adj[dst].add(src)

        # Do reverse BFS from all nodes in target edges to find reachable edges
        queue = deque(reachable_nodes)
        visited = reachable_nodes.copy()

        while queue:
            node = queue.popleft()
            # Add all incoming nodes to queue if not visited
            for prev_node in reverse_adj[node]:
                if prev_node not in visited:
                    visited.add(prev_node)
                    queue.append(prev_node)
                    reachable_nodes.add(prev_node)

        # Filter call_graph to only keep edges between reachable nodes that lead to target edges
        filtered_graph = {}
        for edge, info_dict in call_graph.items():
            src, dst = edge.split("->")
            if src in reachable_nodes and dst in reachable_nodes:
                if "." in src:
                    info = src.split(".")
                    reachable_methods.add(src.split(":")[1])
                    reachable_actors.add(info[0])
                if "." not in src:
                    reachable_funcs.add(src)
                if "." in dst:
                    info = dst.split(".")
                    reachable_methods.add(dst.split(":")[1])
                    reachable_actors.add(info[0])
                if "." not in dst:
                    reachable_funcs.add(dst)
                filtered_graph[edge] = info_dict

        return filtered_graph, reachable_methods, reachable_actors, reachable_funcs

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

    def get_flame_graph_data(self, job_id):
        """Return the flame graph data for a specific job."""
        flame_data = {"aggregated": []}

        # Add aggregated data for flame graph
        visited = {}
        for func_id, func_data in self.flame_graph_aggregated.get(job_id, {}).items():
            if func_id in visited:
                total_in_parent = visited[func_id]
            else:
                total_in_parent = defaultdict(lambda: {"duration": 0, "count": 0})
            start_times = self.start_time_record[job_id][func_id]
            for current_task_id, duration in func_data["durations"].items():
                caller_infos = self.caller_info[job_id][current_task_id]
                for caller_info in caller_infos:
                    caller_class = caller_info["class"]
                    caller_func = caller_info["func"]
                    caller_node_id = (
                        f"{caller_class}.{caller_func}" if caller_class else caller_func
                    )
                    total_in_parent[caller_node_id]["duration"] += duration
                    total_in_parent[caller_node_id]["count"] += 1
                    if "start_time" not in total_in_parent[caller_node_id]:
                        total_in_parent[caller_node_id]["start_time"] = start_times.get(
                            caller_node_id, 0
                        )
            visited[func_id] = total_in_parent

            flame_data["aggregated"].append(
                {
                    "name": func_id,
                    "actor_name": func_data["actor_name"],
                    "value": func_data["total_time"],
                    "count": func_data["call_count"],
                    "total_in_parent": [
                        {
                            "caller_node_id": k,
                            "duration": v["duration"],
                            "count": v["count"],
                            "start_time": v["start_time"],
                        }
                        for k, v in total_in_parent.items()
                    ],
                }
            )

        parent_start_times = []
        for callee_id, start_times in self.start_time_record.get(job_id, {}).items():
            if callee_id not in visited:
                start_times = [
                    {"caller_id": k, "start_time": v}
                    for k, v in start_times.items()
                    if v > 0
                ]
                parent_start_times.append(
                    {"callee_id": callee_id, "start_times": start_times}
                )

        flame_data["parent_start_times"] = parent_start_times

        return flame_data

    def emit_task_end(self, task_record):
        """Record the end of a task execution and calculate duration."""
        job_id = task_record["job_id"]
        caller_class = task_record["caller_class"]
        caller_func = task_record["caller_func"]
        current_task_id = task_record["current_task_id"]
        # Create node_id from caller class and function for parent tracking
        node_id = f"{caller_class}.{caller_func}" if caller_class else caller_func

        self.flow_record[job_id].append(
            {
                "type": "exit",
                "callee_id": node_id,
                "callee_task_id": current_task_id,
            }
        )

        duration = task_record["duration"]

        # Update aggregated data using node_id
        self.flame_graph_aggregated[job_id][node_id]["total_time"] += duration
        self.flame_graph_aggregated[job_id][node_id]["call_count"] += 1
        self.flame_graph_aggregated[job_id][node_id]["durations"].update(
            {
                current_task_id: duration,
            }
        )
        self.flame_graph_aggregated[job_id][node_id]["actor_name"] = task_record[
            "actor_name"
        ]

    async def emit_caller_info(self, caller_info):
        """Record caller info."""
        job_id = caller_info["job_id"]
        current_task_id = caller_info["current_task_id"]
        self.caller_info[job_id][current_task_id].append(
            {
                "class": caller_info["caller_class"],
                "func": caller_info["caller_func"],
                "task_id": caller_info["caller_task_id"],
            }
        )

    # Add the new HTTP handler methods
    async def handle_emit_call_record(self, request):
        """Handle HTTP request for emitting call records."""
        try:
            data = await request.json()
            self.emit_call_record(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_object_record_get(self, request):
        """Handle HTTP request for emitting object get records."""
        try:
            data = await request.json()
            self.emit_object_record_get(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_object_record_put(self, request):
        """Handle HTTP request for emitting object put records."""
        try:
            data = await request.json()
            self.emit_object_record_put(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_context(self, request):
        """Handle HTTP request for emitting context info."""
        try:
            data = await request.json()
            self.emit_context(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_resource_usage(self, request):
        """Handle HTTP request for emitting resource usage."""
        try:
            data = await request.json()
            await self.emit_resource_usage(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_task_end(self, request):
        """Handle HTTP request for emitting task end records."""
        try:
            data = await request.json()
            self.emit_task_end(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )

    async def handle_emit_caller_info(self, request):
        """Handle HTTP request for emitting caller info."""
        try:
            data = await request.json()
            await self.emit_caller_info(data)
            return aiohttp.web.json_response({"status": "success"})
        except Exception as e:
            return aiohttp.web.json_response(
                {"status": "error", "error": str(e)}, status=500
            )


_inner_class_name = "_ray_internal_insight_monitor"
_null_object_id = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"


def _get_current_task_name():
    if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
        current_task_name = ray._private.worker.global_worker.current_task_name
        if current_task_name is not None:
            return current_task_name.split(".")[-1]
    return "_main"


def get_current_task_id():
    try:
        current_task_id = ray._private.worker.global_worker.current_task_id
        if current_task_id.is_nil():
            current_task_id = "_main"
        else:
            current_task_id = current_task_id.hex()
    except:
        current_task_id = "_main"
    return current_task_id


def _get_actor_name():
    if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
        actor_id = ray._private.worker.global_worker.actor_id
        if actor_id.is_nil():
            return None
        return ray._private.worker.global_worker.actor_name
    return None


def _get_caller_class():
    caller_class = None
    try:
        # caller actor can be fetched from the runtime context
        # but it may raise Exception if called in the driver or in a task
        actor_id = ray._private.worker.global_worker.actor_id
        if actor_id.is_nil():
            return None
        caller_actor = ray._private.worker.global_worker.core_worker.get_actor_handle(
            actor_id
        )
        if caller_actor is not None:
            caller_class = (
                caller_actor._ray_actor_creation_function_descriptor.class_name.split(
                    "."
                )[-1]
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


def need_record(caller_class):
    return not (
        caller_class is not None
        and (
            caller_class.startswith(_inner_class_name)
            or caller_class.startswith("JobSupervisor")
        )
    )


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

    if not need_record(callee_class):
        return

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        current_task_id = get_current_task_id()

        # Create a record for this call
        job_id = get_current_job_id()
        call_record = {
            "caller_class": caller_class,
            "caller_func": caller_func,
            "callee_class": callee_class,
            "callee_func": callee_func,
            "call_times": 1,
            "job_id": job_id,
            "current_task_id": current_task_id,
            "start_time": time.time(),
        }

        emit_request("emit-call-record", call_record)
    except Exception as e:
        print(f"Error recording control flow: {e}")


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

    try:
        caller_class = _get_caller_class()

        if not need_record(caller_class):
            return

        recv_func = _get_current_task_name()

        job_id = get_current_job_id()
        object_recv_record = {
            "object_id": object_id,
            "recv_class": caller_class,
            "recv_func": recv_func,
            "timestamp": time.time(),
            "job_id": job_id,
        }

        emit_request("emit-object-record-get", object_recv_record)
    except Exception as e:
        print(f"Error recording object arg get: {e}")


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

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()

        if not need_record(caller_class):
            return

        # Create a record for this call
        job_id = get_current_job_id()
        object_record = {
            "object_id": object_id,
            "size": size,
            "argpos": -2,
            "timestamp": time.time(),
            "caller_class": caller_class,
            "caller_func": caller_func,
            "job_id": job_id,
        }

        emit_request("emit-object-record-put", object_record)
    except Exception as e:
        print(f"Error recording object put: {e}")


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

    try:
        callee_class = None
        callee_info = callee.split(".")
        if len(callee_info) == 2:
            callee_class = None
        elif len(callee_info) == 3:
            callee_class = callee_info[-2]

        if not need_record(callee_class):
            return

        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        # Create a record for this call
        job_id = get_current_job_id()
        object_record = {
            "object_id": object_id,
            "argpos": argpos,
            "size": size,
            "timestamp": time.time(),
            "caller_class": caller_class,
            "caller_func": caller_func,
            "job_id": job_id,
        }

        emit_request("emit-object-record-put", object_record)
    except Exception as e:
        print(f"Error recording object arg put: {e}")


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

    try:
        caller_class = _get_caller_class()

        if not need_record(caller_class):
            return

        # Get the task name from the runtime context
        # if there is no task name, it should be the driver
        caller_func = _get_current_task_name()
        # Create a record for this call
        job_id = get_current_job_id()
        object_record = {
            "object_id": object_id,
            "size": size,
            "argpos": -1,
            "timestamp": time.time(),
            "caller_class": caller_class,
            "caller_func": caller_func,
            "job_id": job_id,
        }

        emit_request("emit-object-record-put", object_record)
    except Exception as e:
        print(f"Error recording object return put: {e}")


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

    try:
        # Get the task name from the runtime context
        # if there is no task name, it should be the driver
        recv_func = _get_current_task_name()
        caller_class = _get_caller_class()

        job_id = get_current_job_id()
        object_recv_record = {
            "object_id": object_id,
            "recv_class": caller_class,
            "recv_func": recv_func,
            "timestamp": time.time(),
            "job_id": job_id,
        }

        if not need_record(caller_class):
            return

        emit_request("emit-object-record-get", object_recv_record)
    except Exception as e:
        print(f"Error recording object get: {e}")


def report_resource_usage(usage: dict):
    """
    report the resource usage of the current task
    usage is a dict of the resource usage
    e.g. {"torch_gram": {"used": 1024, "base": "gpu"}}
    """
    if not is_flow_insight_enabled():
        return

    try:
        current_class = _get_caller_class()
        if current_class is None:
            return
        actor_info = current_class.split(":")
        job_id = get_current_job_id()

        if not need_record(current_class):
            return

        resource_usage_data = {
            "actor_id": actor_info[1],
            "job_id": job_id,
            "usage": usage,
        }

        emit_request("emit-resource-usage", resource_usage_data)
    except Exception as e:
        print(f"Error reporting resource usage: {e}")


def register_current_context(context: dict):
    """
    register the current context info of the current node
    """
    if not is_flow_insight_enabled():
        return

    try:
        current_class = _get_caller_class()
        if current_class is None:
            return
        actor_info = current_class.split(":")

        job_id = get_current_job_id()

        if not need_record(current_class):
            return

        context_data = {
            "actor_id": actor_info[1],
            "job_id": job_id,
            "context": context,
        }

        emit_request("emit-context", context_data)
    except Exception as e:
        print(f"Error registering current context: {e}")


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

    try:
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
    except Exception as e:
        print(f"Error reporting torch gram: {e}")


def record_task_duration(duration):
    """
    Record the duration of a task execution for flame graph visualization.
    This should be called at the end of a task or actor method.
    """
    if not is_flow_insight_enabled():
        return

    if duration is None:
        return

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()

        if not need_record(caller_class):
            return

        actor_name = _get_actor_name()

        current_task_id = get_current_task_id()

        # Create a record for this task end
        job_id = get_current_job_id()
        task_record = {
            "caller_class": caller_class,
            "caller_func": caller_func,
            "actor_name": actor_name,
            "duration": duration,
            "job_id": job_id,
            "current_task_id": current_task_id,
        }

        emit_request("emit-task-end", task_record)
    except Exception as e:
        print(f"Error recording task duration: {e}")
        return


@contextmanager
def timeit():
    """A context manager for recording task execution timing in Ray.

    This context manager automatically records the start and end time of a task
    for flame graph visualization. It should be used within Ray tasks or actor methods.

    Example:
        @ray.remote
        def my_task():
            with timeit():
                # Your task code here
                result = do_work()
                return result

        @ray.remote
        class MyActor:
            def my_method(self):
                with timeit():
                    # Your method code here
                    result = self.do_work()
                    return result
    """
    try:
        start_time = time.time()
        yield
    finally:
        record_task_duration(time.time() - start_time)


def report_trace_info(caller_info):
    """
    Report the trace info of the current task
    """
    if not is_flow_insight_enabled():
        return

    current_task_id = get_current_task_id()

    current_class = _get_caller_class()

    if not need_record(current_class):
        return

    job_id = get_current_job_id()
    trace_info = {
        "job_id": job_id,
        "caller_class": caller_info.get("caller_class"),
        "caller_func": caller_info.get("caller_func"),
        "caller_task_id": caller_info.get("caller_task_id"),
        "current_task_id": current_task_id,
    }

    emit_request("emit-caller-info", trace_info)


def get_caller_info():
    """
    Get the caller info of the current task
    """
    if not is_flow_insight_enabled():
        return
    caller_class = _get_caller_class()
    caller_func = _get_current_task_name()
    caller_task_id = get_current_task_id()
    return {
        "caller_class": caller_class,
        "caller_func": caller_func,
        "caller_task_id": caller_task_id,
    }
