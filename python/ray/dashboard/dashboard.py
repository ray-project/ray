try:
    import aiohttp.web
except ImportError:
    print("The dashboard requires aiohttp to run.")
    import sys
    sys.exit(1)

import argparse
import copy
import datetime
import errno
import json
import logging
import os
import re
import threading
import time
import traceback
import yaml
import uuid

from base64 import b64decode
from collections import defaultdict
from operator import itemgetter
from typing import Dict

import grpc
from google.protobuf.json_format import MessageToDict
import ray
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
import ray.ray_constants as ray_constants

try:
    from ray.tune.result import DEFAULT_RESULTS_DIR
    from ray.tune import Analysis
    from tensorboard import program
except ImportError:
    Analysis = None

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def round_resource_value(quantity):
    if quantity.is_integer():
        return int(quantity)
    else:
        return round(quantity, 2)


def format_resource(resource_name, quantity):
    if resource_name == "object_store_memory" or resource_name == "memory":
        # Convert to 50MiB chunks and then to GiB
        quantity = quantity * (50 * 1024 * 1024) / (1024 * 1024 * 1024)
        return "{} GiB".format(round_resource_value(quantity))
    return "{}".format(round_resource_value(quantity))


def format_reply_id(reply):
    if isinstance(reply, dict):
        for k, v in reply.items():
            if isinstance(v, dict) or isinstance(v, list):
                format_reply_id(v)
            else:
                if k.endswith("Id"):
                    v = b64decode(v)
                    reply[k] = ray.utils.binary_to_hex(v)
    elif isinstance(reply, list):
        for item in reply:
            format_reply_id(item)


def measures_to_dict(measures):
    measures_dict = {}
    for measure in measures:
        tags = measure["tags"].split(",")[-1]
        if "intValue" in measure:
            measures_dict[tags] = measure["intValue"]
        elif "doubleValue" in measure:
            measures_dict[tags] = measure["doubleValue"]
    return measures_dict


def b64_decode(reply):
    return b64decode(reply).decode("utf-8")


class Dashboard(object):
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Attributes:
        redis_client: A client used to communicate with the Redis server.
    """

    def __init__(self,
                 host,
                 port,
                 redis_address,
                 temp_dir,
                 redis_password=None):
        """Initialize the dashboard object."""
        self.host = host
        self.port = port
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)
        self.temp_dir = temp_dir

        self.node_stats = NodeStats(redis_address, redis_password)
        self.raylet_stats = RayletStats(redis_address, redis_password)
        if Analysis is not None:
            self.tune_stats = TuneCollector(DEFAULT_RESULTS_DIR, 2.0)

        # Setting the environment variable RAY_DASHBOARD_DEV=1 disables some
        # security checks in the dashboard server to ease development while
        # using the React dev server. Specifically, when this option is set, we
        # allow cross-origin requests to be made.
        self.is_dev = os.environ.get("RAY_DASHBOARD_DEV") == "1"

        self.app = aiohttp.web.Application()
        self.setup_routes()

    def setup_routes(self):
        def forbidden() -> aiohttp.web.Response:
            return aiohttp.web.Response(status=403, text="403 Forbidden")

        def get_forbidden(_) -> aiohttp.web.Response:
            return forbidden()

        async def get_index(req) -> aiohttp.web.Response:
            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "client/build/index.html"))

        async def get_favicon(req) -> aiohttp.web.Response:
            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "client/build/favicon.ico"))

        async def json_response(result=None, error=None,
                                ts=None) -> aiohttp.web.Response:
            if ts is None:
                ts = datetime.datetime.utcnow()

            headers = None
            if self.is_dev:
                headers = {"Access-Control-Allow-Origin": "*"}

            return aiohttp.web.json_response(
                {
                    "result": result,
                    "timestamp": to_unix_time(ts),
                    "error": error,
                },
                headers=headers)

        async def ray_config(_) -> aiohttp.web.Response:
            try:
                config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
            except Exception:
                return await json_response(error="No config")

            D = {
                "min_workers": cfg["min_workers"],
                "max_workers": cfg["max_workers"],
                "initial_workers": cfg["initial_workers"],
                "autoscaling_mode": cfg["autoscaling_mode"],
                "idle_timeout_minutes": cfg["idle_timeout_minutes"],
            }

            try:
                D["head_type"] = cfg["head_node"]["InstanceType"]
            except KeyError:
                D["head_type"] = "unknown"

            try:
                D["worker_type"] = cfg["worker_nodes"]["InstanceType"]
            except KeyError:
                D["worker_type"] = "unknown"

            return await json_response(result=D)

        async def node_info(req) -> aiohttp.web.Response:
            now = datetime.datetime.utcnow()
            D = self.node_stats.get_node_stats()
            return await json_response(result=D, ts=now)

        async def raylet_info(req) -> aiohttp.web.Response:
            D = self.raylet_stats.get_raylet_stats()
            workers_info_by_node = {
                data["nodeId"]: data.get("workersStats")
                for data in D.values()
            }
            infeasible_tasks = sum(
                (data.get("infeasibleTasks", []) for data in D.values()), [])
            # ready_tasks are used to render tasks that are not schedulable
            # due to resource limitations.
            # (e.g., Actor requires 2 GPUs but there is only 1 gpu available).
            ready_tasks = sum(
                (data.get("readyTasks", []) for data in D.values()), [])
            actor_tree = self.node_stats.get_actor_tree(
                workers_info_by_node, infeasible_tasks, ready_tasks)
            for address, data in D.items():
                # process view data
                measures_dicts = {}
                for view_data in data["viewData"]:
                    view_name = view_data["viewName"]
                    if view_name in ("local_available_resource",
                                     "local_total_resource",
                                     "object_manager_stats"):
                        measures_dicts[view_name] = measures_to_dict(
                            view_data["measures"])
                # process resources info
                extra_info_strings = []
                prefix = "ResourceName:"
                for resource_name, total_resource in measures_dicts[
                        "local_total_resource"].items():
                    available_resource = measures_dicts[
                        "local_available_resource"].get(resource_name, .0)
                    resource_name = resource_name[len(prefix):]
                    extra_info_strings.append("{}: {} / {}".format(
                        resource_name,
                        format_resource(resource_name,
                                        total_resource - available_resource),
                        format_resource(resource_name, total_resource)))
                data["extraInfo"] = ", ".join(extra_info_strings) + "\n"
                if os.environ.get("RAY_DASHBOARD_DEBUG"):
                    # process object store info
                    extra_info_strings = []
                    prefix = "ValueType:"
                    for stats_name in [
                            "used_object_store_memory", "num_local_objects"
                    ]:
                        stats_value = measures_dicts[
                            "object_manager_stats"].get(
                                prefix + stats_name, .0)
                        extra_info_strings.append("{}: {}".format(
                            stats_name, stats_value))
                    data["extraInfo"] += ", ".join(extra_info_strings)
                    # process actor info
                    actor_tree_str = json.dumps(
                        actor_tree, indent=2, sort_keys=True)
                    lines = actor_tree_str.split("\n")
                    max_line_length = max(map(len, lines))
                    to_print = []
                    for line in lines:
                        to_print.append(line +
                                        (max_line_length - len(line)) * " ")
                    data["extraInfo"] += "\n" + "\n".join(to_print)
            result = {"nodes": D, "actors": actor_tree}
            return await json_response(result=result)

        async def tune_info(req) -> aiohttp.web.Response:
            if Analysis is not None:
                D = self.tune_stats.get_stats()
            else:
                D = {}
            return await json_response(result=D)

        async def tune_availability(req) -> aiohttp.web.Response:
            if Analysis is not None:
                D = self.tune_stats.get_availability()
            else:
                D = {"available": False}
            return await json_response(result=D)

        async def launch_profiling(req) -> aiohttp.web.Response:
            node_id = req.query.get("node_id")
            pid = int(req.query.get("pid"))
            duration = int(req.query.get("duration"))
            profiling_id = self.raylet_stats.launch_profiling(
                node_id=node_id, pid=pid, duration=duration)
            return await json_response(str(profiling_id))

        async def check_profiling_status(req) -> aiohttp.web.Response:
            profiling_id = req.query.get("profiling_id")
            return await json_response(
                self.raylet_stats.check_profiling_status(profiling_id))

        async def get_profiling_info(req) -> aiohttp.web.Response:
            profiling_id = req.query.get("profiling_id")
            return aiohttp.web.json_response(
                self.raylet_stats.get_profiling_info(profiling_id))

        async def kill_actor(req) -> aiohttp.web.Response:
            actor_id = req.query.get("actor_id")
            ip_address = req.query.get("ip_address")
            port = req.query.get("port")
            return await json_response(
                self.raylet_stats.kill_actor(actor_id, ip_address, port))

        async def logs(req) -> aiohttp.web.Response:
            hostname = req.query.get("hostname")
            pid = req.query.get("pid")
            result = self.node_stats.get_logs(hostname, pid)
            return await json_response(result=result)

        async def errors(req) -> aiohttp.web.Response:
            hostname = req.query.get("hostname")
            pid = req.query.get("pid")
            result = self.node_stats.get_errors(hostname, pid)
            return await json_response(result=result)

        self.app.router.add_get("/", get_index)
        self.app.router.add_get("/favicon.ico", get_favicon)

        build_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "client/build")
        if not os.path.isdir(build_dir):
            raise OSError(
                errno.ENOENT,
                "Dashboard build directory not found. If installing "
                "from source, please follow the additional steps required to "
                "build the dashboard "
                "(cd python/ray/dashboard/client && npm ci && npm run build)",
                build_dir)

        static_dir = os.path.join(build_dir, "static")
        self.app.router.add_static("/static", static_dir)

        speedscope_dir = os.path.join(build_dir, "speedscope-1.5.3")
        self.app.router.add_static("/speedscope", speedscope_dir)

        self.app.router.add_get("/api/ray_config", ray_config)
        self.app.router.add_get("/api/node_info", node_info)
        self.app.router.add_get("/api/raylet_info", raylet_info)
        self.app.router.add_get("/api/tune_info", tune_info)
        self.app.router.add_get("/api/tune_availability", tune_availability)
        self.app.router.add_get("/api/launch_profiling", launch_profiling)
        self.app.router.add_get("/api/check_profiling_status",
                                check_profiling_status)
        self.app.router.add_get("/api/get_profiling_info", get_profiling_info)
        self.app.router.add_get("/api/kill_actor", kill_actor)
        self.app.router.add_get("/api/logs", logs)
        self.app.router.add_get("/api/errors", errors)

        self.app.router.add_get("/{_}", get_forbidden)

    def log_dashboard_url(self):
        url = ray.services.get_webui_url_from_redis(self.redis_client)
        with open(os.path.join(self.temp_dir, "dashboard_url"), "w") as f:
            f.write(url)
        logger.info("Dashboard running on {}".format(url))

    def run(self):
        self.log_dashboard_url()
        self.node_stats.start()
        self.raylet_stats.start()
        if Analysis is not None:
            self.tune_stats.start()
        aiohttp.web.run_app(self.app, host=self.host, port=self.port)


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._addr_to_owner_addr = {}
        self._addr_to_actor_id = {}
        self._addr_to_extra_info_dict = {}
        self._node_stats_lock = threading.Lock()

        self._default_info = {
            "actorId": "",
            "children": {},
            "currentTaskFuncDesc": [],
            "ipAddress": "",
            "isDirectCall": False,
            "jobId": "",
            "numExecutedTasks": 0,
            "numLocalObjects": 0,
            "numObjectIdsInScope": 0,
            "port": 0,
            "state": 0,
            "taskQueueLength": 0,
            "usedObjectStoreMemory": 0,
            "usedResources": {},
        }

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.state.state._initialize_global_state(
            redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def calculate_log_counts(self):
        return {
            ip: {
                pid: len(logs_for_pid)
                for pid, logs_for_pid in logs_for_ip.items()
            }
            for ip, logs_for_ip in self._logs.items()
        }

    def calculate_error_counts(self):
        return {
            ip: {
                pid: len(errors_for_pid)
                for pid, errors_for_pid in errors_for_ip.items()
            }
            for ip, errors_for_ip in self._errors.items()
        }

    def purge_outdated_stats(self):
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = to_unix_time(datetime.datetime.utcnow())
        self._node_stats = {
            k: v
            for k, v in self._node_stats.items() if current(v["now"], now)
        }

    def get_node_stats(self) -> Dict:
        with self._node_stats_lock:
            self.purge_outdated_stats()
            node_stats = sorted(
                (v for v in self._node_stats.values()),
                key=itemgetter("boot_time"))
            return {
                "clients": node_stats,
                "log_counts": self.calculate_log_counts(),
                "error_counts": self.calculate_error_counts(),
            }

    def get_actor_tree(self, workers_info_by_node, infeasible_tasks,
                       ready_tasks) -> Dict:
        now = time.time()
        # construct flattened actor tree
        flattened_tree = {"root": {"children": {}}}
        child_to_parent = {}
        with self._node_stats_lock:
            for addr, actor_id in self._addr_to_actor_id.items():
                flattened_tree[actor_id] = copy.deepcopy(self._default_info)
                flattened_tree[actor_id].update(
                    self._addr_to_extra_info_dict[addr])
                parent_id = self._addr_to_actor_id.get(
                    self._addr_to_owner_addr[addr], "root")
                child_to_parent[actor_id] = parent_id

            for node_id, workers_info in workers_info_by_node.items():
                for worker_info in workers_info:
                    if "coreWorkerStats" in worker_info:
                        core_worker_stats = worker_info["coreWorkerStats"]
                        addr = (core_worker_stats["ipAddress"],
                                str(core_worker_stats["port"]))
                        if addr in self._addr_to_actor_id:
                            actor_info = flattened_tree[self._addr_to_actor_id[
                                addr]]
                            format_reply_id(core_worker_stats)
                            actor_info.update(core_worker_stats)
                            actor_info["averageTaskExecutionSpeed"] = round(
                                actor_info["numExecutedTasks"] /
                                (now - actor_info["timestamp"] / 1000), 2)
                            actor_info["nodeId"] = node_id
                            actor_info["pid"] = worker_info["pid"]

            def _update_flatten_tree(task, task_spec_type, invalid_state_type):
                actor_id = ray.utils.binary_to_hex(
                    b64decode(task[task_spec_type]["actorId"]))
                caller_addr = (task["callerAddress"]["ipAddress"],
                               str(task["callerAddress"]["port"]))
                caller_id = self._addr_to_actor_id.get(caller_addr, "root")
                child_to_parent[actor_id] = caller_id
                task["state"] = -1
                task["invalidStateType"] = invalid_state_type
                task["actorTitle"] = task["functionDescriptor"][
                    "pythonFunctionDescriptor"]["className"]
                format_reply_id(task)
                flattened_tree[actor_id] = task

            for infeasible_task in infeasible_tasks:
                _update_flatten_tree(infeasible_task, "actorCreationTaskSpec",
                                     "infeasibleActor")

            for ready_task in ready_tasks:
                _update_flatten_tree(ready_task, "actorCreationTaskSpec",
                                     "pendingActor")

        # construct actor tree
        actor_tree = flattened_tree
        for actor_id, parent_id in child_to_parent.items():
            actor_tree[parent_id]["children"][actor_id] = actor_tree[actor_id]
        return actor_tree["root"]["children"]

    def get_logs(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        logs = self._logs.get(ip, {})
        if pid:
            logs = {pid: logs.get(pid, [])}
        return logs

    def get_errors(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        errors = self._errors.get(ip, {})
        if pid:
            errors = {pid: errors.get(pid, [])}
        return errors

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)

        p.psubscribe(self.redis_key)
        logger.info("NodeStats: subscribed to {}".format(self.redis_key))

        log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
        p.subscribe(log_channel)
        logger.info("NodeStats: subscribed to {}".format(log_channel))

        error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        p.subscribe(error_channel)
        logger.info("NodeStats: subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        p.subscribe(actor_channel)
        logger.info("NodeStats: subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        with self._node_stats_lock:
            for actor_data in current_actor_table.values():
                addr = (actor_data["Address"]["IPAddress"],
                        str(actor_data["Address"]["Port"]))
                owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                              str(actor_data["OwnerAddress"]["Port"]))
                self._addr_to_owner_addr[addr] = owner_addr
                self._addr_to_actor_id[addr] = actor_data["ActorID"]
                self._addr_to_extra_info_dict[addr] = {
                    "jobId": actor_data["JobID"],
                    "state": actor_data["State"],
                    "isDirectCall": actor_data["IsDirectCall"],
                    "timestamp": actor_data["Timestamp"]
                }

        for x in p.listen():
            try:
                with self._node_stats_lock:
                    channel = ray.utils.decode(x["channel"])
                    data = x["data"]
                    if channel == log_channel:
                        data = json.loads(ray.utils.decode(data))
                        ip = data["ip"]
                        pid = str(data["pid"])
                        self._logs[ip][pid].extend(data["lines"])
                    elif channel == str(error_channel):
                        gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                        error_data = ray.gcs_utils.ErrorTableData.FromString(
                            gcs_entry.entries[0])
                        message = error_data.error_message
                        message = re.sub(r"\x1b\[\d+m", "", message)
                        match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                        if match:
                            pid = match.group(1)
                            ip = match.group(2)
                            self._errors[ip][pid].append({
                                "message": message,
                                "timestamp": error_data.timestamp,
                                "type": error_data.type
                            })
                    elif channel == str(actor_channel):
                        gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                        actor_data = ray.gcs_utils.ActorTableData.FromString(
                            gcs_entry.entries[0])
                        addr = (actor_data.address.ip_address,
                                str(actor_data.address.port))
                        owner_addr = (actor_data.owner_address.ip_address,
                                      str(actor_data.owner_address.port))
                        self._addr_to_owner_addr[addr] = owner_addr
                        self._addr_to_actor_id[addr] = ray.utils.binary_to_hex(
                            actor_data.actor_id)
                        self._addr_to_extra_info_dict[addr] = {
                            "jobId": ray.utils.binary_to_hex(
                                actor_data.job_id),
                            "state": actor_data.state,
                            "isDirectCall": True,
                            "timestamp": actor_data.timestamp
                        }
                    else:
                        data = json.loads(ray.utils.decode(data))
                        self._node_stats[data["hostname"]] = data
            except Exception:
                logger.exception(traceback.format_exc())
                continue


class RayletStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.nodes_lock = threading.Lock()
        self.nodes = []
        self.stubs = {}
        self.reporter_stubs = {}
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._raylet_stats_lock = threading.Lock()
        self._raylet_stats = {}
        self._profiling_stats = {}

        self.update_nodes()

        super().__init__()

    def update_nodes(self):
        with self.nodes_lock:
            self.nodes = ray.nodes()
            node_ids = [node["NodeID"] for node in self.nodes]

            # First remove node connections of disconnected nodes.
            for node_id in self.stubs.keys():
                if node_id not in node_ids:
                    stub = self.stubs.pop(node_id)
                    stub.close()
                    reporter_stub = self.reporter_stubs.pop(node_id)
                    reporter_stub.close()

            # Now add node connections of new nodes.
            for node in self.nodes:
                node_id = node["NodeID"]
                if node_id not in self.stubs:
                    node_ip = node["NodeManagerAddress"]
                    channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, node["NodeManagerPort"]))
                    stub = node_manager_pb2_grpc.NodeManagerServiceStub(
                        channel)
                    self.stubs[node_id] = stub
                    # Block wait until the reporter for the node starts.
                    while True:
                        reporter_port = self.redis_client.get(
                            "REPORTER_PORT:{}".format(node_ip))
                        if reporter_port:
                            break
                    reporter_channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, int(reporter_port)))
                    reporter_stub = reporter_pb2_grpc.ReporterServiceStub(
                        reporter_channel)
                    self.reporter_stubs[node_id] = reporter_stub

            assert len(self.stubs) == len(
                self.reporter_stubs), (self.stubs.keys(),
                                       self.reporter_stubs.keys())

    def get_raylet_stats(self) -> Dict:
        with self._raylet_stats_lock:
            return copy.deepcopy(self._raylet_stats)

    def launch_profiling(self, node_id, pid, duration):
        profiling_id = str(uuid.uuid4())

        def _callback(reply_future):
            reply = reply_future.result()
            with self._raylet_stats_lock:
                self._profiling_stats[profiling_id] = reply

        reporter_stub = self.reporter_stubs[node_id]
        reply_future = reporter_stub.GetProfilingStats.future(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        reply_future.add_done_callback(_callback)
        return profiling_id

    def check_profiling_status(self, profiling_id):
        with self._raylet_stats_lock:
            is_present = profiling_id in self._profiling_stats
        if is_present:
            reply = self._profiling_stats[profiling_id]
            if reply.stderr:
                return {"status": "error", "error": reply.stderr}
            else:
                return {"status": "finished"}
        else:
            return {"status": "pending"}

    def get_profiling_info(self, profiling_id):
        with self._raylet_stats_lock:
            profiling_stats = self._profiling_stats.get(profiling_id)
        assert profiling_stats, "profiling not finished"
        return json.loads(profiling_stats.profiling_stats)

    def kill_actor(self, actor_id, ip_address, port):
        channel = grpc.insecure_channel("{}:{}".format(ip_address, int(port)))
        stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

        def _callback(reply_future):
            _ = reply_future.result()

        reply_future = stub.KillActor.future(
            core_worker_pb2.KillActorRequest(
                intended_actor_id=ray.utils.hex_to_binary(actor_id)))
        reply_future.add_done_callback(_callback)
        return {}

    def run(self):
        counter = 0
        while True:
            time.sleep(1.0)
            replies = {}
            for node in self.nodes:
                node_id = node["NodeID"]
                stub = self.stubs[node_id]
                reply = stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(), timeout=2)
                reply_dict = MessageToDict(reply)
                reply_dict["nodeId"] = node_id
                replies[node["NodeManagerAddress"]] = reply_dict
            with self._raylet_stats_lock:
                for address, reply_dict in replies.items():
                    self._raylet_stats[address] = reply_dict
            counter += 1
            # From time to time, check if new nodes have joined the cluster
            # and update self.nodes
            if counter % 10:
                self.update_nodes()


class TuneCollector(threading.Thread):
    """Initialize collector worker thread.
    Args
        logdir (str): Directory path to save the status information of
                        jobs and trials.
        reload_interval (float): Interval(in s) of space between loading
                        data from logs
    """

    def __init__(self, logdir, reload_interval):
        super().__init__()
        self._logdir = logdir
        self._trial_records = {}
        self._data_lock = threading.Lock()
        self._reload_interval = reload_interval
        self._available = False
        self._tensor_board_started = False

    def get_stats(self):
        with self._data_lock:
            return {"trial_records": copy.deepcopy(self._trial_records)}

    def get_availability(self):
        with self._data_lock:
            return {"available": self._available}

    def run(self):
        while True:
            with self._data_lock:
                self.collect()
            time.sleep(self._reload_interval)

    def collect(self):
        """
        Collects and cleans data on the running Tune experiment from the
        Tune logs so that users can see this information in the front-end
        client
        """
        sub_dirs = os.listdir(self._logdir)
        job_names = filter(
            lambda d: os.path.isdir(os.path.join(self._logdir, d)), sub_dirs)

        self._trial_records = {}

        # search through all the sub_directories in log directory
        for job_name in job_names:
            analysis = Analysis(str(os.path.join(self._logdir, job_name)))
            df = analysis.dataframe()
            if len(df) == 0:
                continue

            # start TensorBoard server if not started yet
            if not self._tensor_board_started:
                tb = program.TensorBoard()
                tb.configure(argv=[None, "--logdir", self._logdir])
                tb.launch()
                self._tensor_board_started = True

            self._available = True

            # make sure that data will convert to JSON without error
            df["trial_id"] = df["trial_id"].astype(str)
            df = df.fillna(0)

            # convert df to python dict
            df = df.set_index("trial_id")
            trial_data = df.to_dict(orient="index")

            # clean data and update class attribute
            if len(trial_data) > 0:
                trial_data = self.clean_trials(trial_data, job_name)
                self._trial_records.update(trial_data)

    def clean_trials(self, trial_details, job_name):
        first_trial = trial_details[list(trial_details.keys())[0]]
        config_keys = []
        float_keys = []
        metric_keys = []

        # list of static attributes for trial
        default_names = [
            "logdir", "time_this_iter_s", "done", "episodes_total",
            "training_iteration", "timestamp", "timesteps_total",
            "experiment_id", "date", "timestamp", "time_total_s", "pid",
            "hostname", "node_ip", "time_since_restore",
            "timesteps_since_restore", "iterations_since_restore",
            "experiment_tag"
        ]

        # filter attributes into floats, metrics, and config variables
        for key, value in first_trial.items():
            if isinstance(value, float):
                float_keys.append(key)
            if str(key).startswith("config/"):
                config_keys.append(key)
            elif key not in default_names:
                metric_keys.append(key)

        # clean data into a form that front-end client can handle
        for trial, details in trial_details.items():
            ts = os.path.getctime(details["logdir"])
            formatted_time = datetime.datetime.fromtimestamp(ts).strftime(
                "%Y-%m-%d %H:%M:%S")
            details["start_time"] = formatted_time
            details["params"] = {}
            details["metrics"] = {}

            # round all floats
            for key in float_keys:
                details[key] = round(details[key], 3)

            # group together config attributes
            for key in config_keys:
                new_name = key[7:]
                details["params"][new_name] = details[key]
                details.pop(key)

            # group together metric attributes
            for key in metric_keys:
                details["metrics"][key] = details[key]
                details.pop(key)

            if details["done"]:
                details["status"] = "TERMINATED"
            else:
                details["status"] = "RUNNING"
            details.pop("done")

            details["trial_id"] = trial
            details["job_id"] = job_name

        return trial_details


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "dashboard to connect to."))
    parser.add_argument(
        "--host",
        required=True,
        type=str,
        help="The host to use for the HTTP server.")
    parser.add_argument(
        "--port",
        required=True,
        type=int,
        help="The port to use for the HTTP server.")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="The address to use for Redis.")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="the password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
        "--temp-dir",
        required=False,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.")
    args = parser.parse_args()
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    try:
        dashboard = Dashboard(
            args.host,
            args.port,
            args.redis_address,
            args.temp_dir,
            redis_password=args.redis_password,
        )
        dashboard.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The dashboard on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        if isinstance(e, OSError) and e.errno == errno.ENOENT:
            logger.warning(message)
        else:
            raise e
