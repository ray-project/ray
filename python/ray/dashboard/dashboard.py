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
import platform
import threading
import time
import traceback
import yaml
import uuid
import grpc
from google.protobuf.json_format import MessageToDict
import ray
import ray.ray_constants as ray_constants

from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.dashboard.interface import BaseDashboardController
from ray.dashboard.interface import BaseDashboardRouteHandler
from ray.dashboard.memory import construct_memory_table, MemoryTable
from ray.dashboard.metrics_exporter.client import Exporter
from ray.dashboard.metrics_exporter.client import MetricsExportClient
from ray.dashboard.node_stats import NodeStats
from ray.dashboard.util import to_unix_time, measures_to_dict, format_resource

try:
    from ray.tune import Analysis
    from tensorboard import program
except ImportError:
    Analysis = None

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


async def json_response(is_dev, result=None, error=None,
                        ts=None) -> aiohttp.web.Response:
    if ts is None:
        ts = datetime.datetime.utcnow()

    headers = None
    if is_dev:
        headers = {"Access-Control-Allow-Origin": "*"}

    return aiohttp.web.json_response(
        {
            "result": result,
            "timestamp": to_unix_time(ts),
            "error": error,
        },
        headers=headers)


class DashboardController(BaseDashboardController):
    def __init__(self, redis_address, redis_password):
        self.node_stats = NodeStats(redis_address, redis_password)
        self.raylet_stats = RayletStats(
            redis_address, redis_password=redis_password)
        if Analysis is not None:
            self.tune_stats = TuneCollector(2.0)
        self.memory_table = MemoryTable([])

    def _construct_raylet_info(self):
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
        ready_tasks = sum((data.get("readyTasks", []) for data in D.values()),
                          [])
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
                    stats_value = measures_dicts["object_manager_stats"].get(
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
                    to_print.append(line + (max_line_length - len(line)) * " ")
                data["extraInfo"] += "\n" + "\n".join(to_print)
        return {"nodes": D, "actors": actor_tree}

    def get_ray_config(self):
        try:
            config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
        except Exception:
            error = "No config"
            return error, None

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

        return None, D

    def get_node_info(self):
        return self.node_stats.get_node_stats()

    def get_raylet_info(self):
        return self._construct_raylet_info()

    def get_memory_table_info(self) -> MemoryTable:
        # Collecting memory info adds big overhead to the cluster.
        # This must be collected only when it is necessary.
        self.raylet_stats.include_memory_info = True
        D = self.raylet_stats.get_raylet_stats()
        workers_info_by_node = {
            data["nodeId"]: data.get("workersStats")
            for data in D.values()
        }
        self.memory_table = construct_memory_table(workers_info_by_node)
        return self.memory_table

    def stop_collecting_memory_table_info(self):
        self.raylet_stats.include_memory_info = False

    def tune_info(self):
        if Analysis is not None:
            D = self.tune_stats.get_stats()
        else:
            D = {}
        return D

    def tune_availability(self):
        if Analysis is not None:
            D = self.tune_stats.get_availability()
        else:
            D = {"available": False, "trials_available": False}
        return D

    def set_tune_experiment(self, experiment):
        if Analysis is not None:
            return self.tune_stats.set_experiment(experiment)
        return "Tune Not Enabled", None

    def enable_tune_tensorboard(self):
        if Analysis is not None:
            self.tune_stats.enable_tensorboard()

    def launch_profiling(self, node_id, pid, duration):
        profiling_id = self.raylet_stats.launch_profiling(
            node_id=node_id, pid=pid, duration=duration)
        return profiling_id

    def check_profiling_status(self, profiling_id):
        return self.raylet_stats.check_profiling_status(profiling_id)

    def get_profiling_info(self, profiling_id):
        return self.raylet_stats.get_profiling_info(profiling_id)

    def kill_actor(self, actor_id, ip_address, port):
        return self.raylet_stats.kill_actor(actor_id, ip_address, port)

    def get_logs(self, hostname, pid):
        return self.node_stats.get_logs(hostname, pid)

    def get_errors(self, hostname, pid):
        return self.node_stats.get_errors(hostname, pid)

    def start_collecting_metrics(self):
        self.node_stats.start()
        self.raylet_stats.start()
        if Analysis is not None:
            self.tune_stats.start()


class DashboardRouteHandler(BaseDashboardRouteHandler):
    def __init__(self, dashboard_controller: DashboardController,
                 is_dev=False):
        self.dashboard_controller = dashboard_controller
        self.is_dev = is_dev

    def forbidden(self) -> aiohttp.web.Response:
        return aiohttp.web.Response(status=403, text="403 Forbidden")

    async def get_forbidden(self, _) -> aiohttp.web.Response:
        return self.forbidden()

    async def get_index(self, req) -> aiohttp.web.Response:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "client/build/index.html"))

    async def get_favicon(self, req) -> aiohttp.web.Response:
        return aiohttp.web.FileResponse(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "client/build/favicon.ico"))

    async def ray_config(self, req) -> aiohttp.web.Response:
        error, result = self.dashboard_controller.get_ray_config()
        if error:
            return await json_response(self.is_dev, error=error)
        return await json_response(self.is_dev, result=result)

    async def node_info(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        D = self.dashboard_controller.get_node_info()
        return await json_response(self.is_dev, result=D, ts=now)

    async def raylet_info(self, req) -> aiohttp.web.Response:
        result = self.dashboard_controller.get_raylet_info()
        return await json_response(self.is_dev, result=result)

    async def memory_table_info(self, req) -> aiohttp.web.Response:
        memory_table = self.dashboard_controller.get_memory_table_info()
        return await json_response(self.is_dev, result=memory_table.__dict__())

    async def stop_collecting_memory_table_info(self,
                                                req) -> aiohttp.web.Response:
        self.dashboard_controller.stop_collecting_memory_table_info()
        return await json_response(self.is_dev, result={})

    async def tune_info(self, req) -> aiohttp.web.Response:
        result = self.dashboard_controller.tune_info()
        return await json_response(self.is_dev, result=result)

    async def tune_availability(self, req) -> aiohttp.web.Response:
        result = self.dashboard_controller.tune_availability()
        return await json_response(self.is_dev, result=result)

    async def set_tune_experiment(self, req) -> aiohttp.web.Response:
        data = await req.json()
        error, result = self.dashboard_controller.set_tune_experiment(
            data["experiment"])
        if error:
            return await json_response(self.is_dev, error=error)
        return await json_response(self.is_dev, result=result)

    async def enable_tune_tensorboard(self, req) -> aiohttp.web.Response:
        self.dashboard_controller.enable_tune_tensorboard()
        return await json_response(self.is_dev, result={})

    async def launch_profiling(self, req) -> aiohttp.web.Response:
        node_id = req.query.get("node_id")
        pid = int(req.query.get("pid"))
        duration = int(req.query.get("duration"))
        profiling_id = self.dashboard_controller.launch_profiling(
            node_id, pid, duration)
        return await json_response(self.is_dev, result=str(profiling_id))

    async def check_profiling_status(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        status = self.dashboard_controller.check_profiling_status(profiling_id)
        return await json_response(self.is_dev, result=status)

    async def get_profiling_info(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        profiling_info = self.dashboard_controller.get_profiling_info(
            profiling_id)
        return aiohttp.web.json_response(profiling_info)

    async def kill_actor(self, req) -> aiohttp.web.Response:
        actor_id = req.query.get("actor_id")
        ip_address = req.query.get("ip_address")
        port = req.query.get("port")
        return await json_response(
            self.is_dev,
            self.dashboard_controller.kill_actor(actor_id, ip_address, port))

    async def logs(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self.dashboard_controller.get_logs(hostname, pid)
        return await json_response(self.is_dev, result=result)

    async def errors(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self.dashboard_controller.get_errors(hostname, pid)
        return await json_response(self.is_dev, result=result)


class MetricsExportHandler:
    def __init__(self,
                 dashboard_controller: DashboardController,
                 metrics_export_client: MetricsExportClient,
                 dashboard_id,
                 is_dev=False):
        assert metrics_export_client is not None
        self.metrics_export_client = metrics_export_client
        self.dashboard_controller = dashboard_controller
        self.is_dev = is_dev

    async def enable_export_metrics(self, req) -> aiohttp.web.Response:
        if self.metrics_export_client.enabled:
            return await json_response(
                self.is_dev, result={"url": None}, error="Already enabled")

        succeed, error = self.metrics_export_client.start_exporting_metrics()
        error_msg = "Failed to enable it. Error: {}".format(error)
        if not succeed:
            return await json_response(
                self.is_dev, result={"url": None}, error=error_msg)

        url = self.metrics_export_client.dashboard_url
        return await json_response(self.is_dev, result={"url": url})

    async def get_dashboard_address(self, req) -> aiohttp.web.Response:
        if not self.metrics_export_client.enabled:
            return await json_response(
                self.is_dev,
                result={"url": None},
                error="Metrics exporting is not enabled.")

        url = self.metrics_export_client.dashboard_url
        return await json_response(self.is_dev, result={"url": url})

    async def redirect_to_dashboard(self, req) -> aiohttp.web.Response:
        if not self.metrics_export_client.enabled:
            return await json_response(
                self.is_dev,
                result={"url": None},
                error="You should enable metrics export to use this endpoint.")

        raise aiohttp.web.HTTPFound(self.metrics_export_client.dashboard_url)


def setup_metrics_export_routes(app: aiohttp.web.Application,
                                handler: MetricsExportHandler):
    """Routes that require dynamically changing class attributes."""
    app.router.add_get("/api/metrics/enable", handler.enable_export_metrics)
    app.router.add_get("/api/metrics/url", handler.get_dashboard_address)
    app.router.add_get("/metrics/redirect", handler.redirect_to_dashboard)


def setup_static_dir(app):
    build_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "client/build")
    if not os.path.isdir(build_dir):
        raise OSError(
            errno.ENOENT, "Dashboard build directory not found. If installing "
            "from source, please follow the additional steps "
            "required to build the dashboard"
            "(cd python/ray/dashboard/client "
            "&& npm ci "
            "&& npm run build)", build_dir)

    static_dir = os.path.join(build_dir, "static")
    app.router.add_static("/static", static_dir)
    return build_dir


def setup_speedscope_dir(app, build_dir):
    speedscope_dir = os.path.join(build_dir, "speedscope-1.5.3")
    app.router.add_static("/speedscope", speedscope_dir)


def setup_dashboard_route(app: aiohttp.web.Application,
                          handler: BaseDashboardRouteHandler,
                          index=None,
                          favicon=None,
                          ray_config=None,
                          node_info=None,
                          raylet_info=None,
                          tune_info=None,
                          tune_availability=None,
                          launch_profiling=None,
                          check_profiling_status=None,
                          get_profiling_info=None,
                          kill_actor=None,
                          logs=None,
                          errors=None,
                          memory_table=None,
                          stop_memory_table=None):
    def add_get_route(route, handler_func):
        if route is not None:
            app.router.add_get(route, handler_func)

    add_get_route(index, handler.get_index)
    add_get_route(favicon, handler.get_favicon)
    add_get_route(ray_config, handler.ray_config)
    add_get_route(node_info, handler.node_info)
    add_get_route(raylet_info, handler.raylet_info)
    add_get_route(tune_info, handler.tune_info)
    add_get_route(tune_availability, handler.tune_availability)
    add_get_route(launch_profiling, handler.launch_profiling)
    add_get_route(check_profiling_status, handler.check_profiling_status)
    add_get_route(get_profiling_info, handler.get_profiling_info)
    add_get_route(kill_actor, handler.kill_actor)
    add_get_route(logs, handler.logs)
    add_get_route(errors, handler.errors)
    add_get_route(memory_table, handler.memory_table_info)
    add_get_route(stop_memory_table, handler.stop_collecting_memory_table_info)


class Dashboard:
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Args:
        host(str): Host address of dashboard aiohttp server.
        port(str): Port number of dashboard aiohttp server.
        redis_address(str): GCS address of a Ray cluster
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        redis_passord(str): Redis password to access GCS
        metrics_export_address(str): The address users host their dashboard.
    """

    def __init__(self,
                 host,
                 port,
                 redis_address,
                 temp_dir,
                 redis_password=None,
                 metrics_export_address=None):
        self.host = host
        self.port = port
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)
        self.temp_dir = temp_dir
        self.dashboard_id = str(uuid.uuid4())
        self.dashboard_controller = DashboardController(
            redis_address, redis_password)

        # Setting the environment variable RAY_DASHBOARD_DEV=1 disables some
        # security checks in the dashboard server to ease development while
        # using the React dev server. Specifically, when this option is set, we
        # allow cross-origin requests to be made.
        self.is_dev = os.environ.get("RAY_DASHBOARD_DEV") == "1"

        self.app = aiohttp.web.Application()
        route_handler = DashboardRouteHandler(
            self.dashboard_controller, is_dev=self.is_dev)

        # Setup Metrics exporting service if necessary.
        self.metrics_export_address = metrics_export_address
        if self.metrics_export_address:
            self._setup_metrics_export()

        # Setup Dashboard Routes
        build_dir = setup_static_dir(self.app)
        setup_speedscope_dir(self.app, build_dir)
        setup_dashboard_route(
            self.app,
            route_handler,
            index="/",
            favicon="/favicon.ico",
            ray_config="/api/ray_config",
            node_info="/api/node_info",
            raylet_info="/api/raylet_info",
            tune_info="/api/tune_info",
            tune_availability="/api/tune_availability",
            launch_profiling="/api/launch_profiling",
            check_profiling_status="/api/check_profiling_status",
            get_profiling_info="/api/get_profiling_info",
            kill_actor="/api/kill_actor",
            logs="/api/logs",
            errors="/api/errors",
            memory_table="/api/memory_table",
            stop_memory_table="/api/stop_memory_table")
        self.app.router.add_get("/{_}", route_handler.get_forbidden)
        self.app.router.add_post("/api/set_tune_experiment",
                                 route_handler.set_tune_experiment)
        self.app.router.add_post("/api/enable_tune_tensorboard",
                                 route_handler.enable_tune_tensorboard)

    def _setup_metrics_export(self):
        exporter = Exporter(self.dashboard_id, self.metrics_export_address,
                            self.dashboard_controller)
        self.metrics_export_client = MetricsExportClient(
            self.metrics_export_address, self.dashboard_controller,
            self.dashboard_id, exporter)

        # Setup endpoints
        metrics_export_handler = MetricsExportHandler(
            self.dashboard_controller,
            self.metrics_export_client,
            self.dashboard_id,
            is_dev=self.is_dev)
        setup_metrics_export_routes(self.app, metrics_export_handler)

    def _start_exporting_metrics(self):
        result, error = self.metrics_export_client.start_exporting_metrics()
        if not result and error:
            url = ray.services.get_webui_url_from_redis(self.redis_client)
            error += (" Please reenable the metrics export by going to "
                      "the url: {}/api/metrics/enable".format(url))
            ray.utils.push_error_to_driver_through_redis(
                self.redis_client, "metrics export failed", error)

    def log_dashboard_url(self):
        url = ray.services.get_webui_url_from_redis(self.redis_client)
        if url is None:
            raise ValueError("WebUI URL is not present in GCS.")
        with open(os.path.join(self.temp_dir, "dashboard_url"), "w") as f:
            f.write(url)
        logger.info("Dashboard running on {}".format(url))

    def run(self):
        self.log_dashboard_url()
        self.dashboard_controller.start_collecting_metrics()
        if self.metrics_export_address:
            self._start_exporting_metrics()
        aiohttp.web.run_app(self.app, host=self.host, port=self.port)


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

        self._update_nodes()
        self.include_memory_info = False

        super().__init__()

    def _update_nodes(self):
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

    def get_raylet_stats(self):
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
        if not is_present:
            return {"status": "pending"}

        reply = self._profiling_stats[profiling_id]
        if reply.stderr:
            return {"status": "error", "error": reply.stderr}
        else:
            return {"status": "finished"}

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
            try:
                for node in self.nodes:
                    node_id = node["NodeID"]
                    stub = self.stubs[node_id]
                    reply = stub.GetNodeStats(
                        node_manager_pb2.GetNodeStatsRequest(
                            include_memory_info=self.include_memory_info),
                        timeout=2)
                    reply_dict = MessageToDict(reply)
                    reply_dict["nodeId"] = node_id
                    replies[node["NodeManagerAddress"]] = reply_dict
                with self._raylet_stats_lock:
                    for address, reply_dict in replies.items():
                        self._raylet_stats[address] = reply_dict
            except Exception:
                logger.exception(traceback.format_exc())
            finally:
                counter += 1
                # From time to time, check if new nodes have joined the cluster
                # and update self.nodes
                if counter % 10:
                    self._update_nodes()


class TuneCollector(threading.Thread):
    """Initialize collector worker thread.
    Args
        logdir (str): Directory path to save the status information of
                        jobs and trials.
        reload_interval (float): Interval(in s) of space between loading
                        data from logs
    """

    def __init__(self, reload_interval):
        self._logdir = None
        self._trial_records = {}
        self._data_lock = threading.Lock()
        self._reload_interval = reload_interval
        self._trials_available = False
        self._tensor_board_dir = ""
        self._enable_tensor_board = False
        self._errors = {}

        super().__init__()

    def get_stats(self):
        with self._data_lock:
            tensor_board_info = {
                "tensorboard_current": self._logdir == self._tensor_board_dir,
                "tensorboard_enabled": self._tensor_board_dir != ""
            }
            return {
                "trial_records": copy.deepcopy(self._trial_records),
                "errors": copy.deepcopy(self._errors),
                "tensorboard": tensor_board_info
            }

    def set_experiment(self, experiment):
        with self._data_lock:
            if os.path.isdir(os.path.expanduser(experiment)):
                self._logdir = os.path.expanduser(experiment)
                return None, {"experiment": self._logdir}
            else:
                return "Not a Valid Directory", None

    def enable_tensorboard(self):
        with self._data_lock:
            if not self._tensor_board_dir:
                tb = program.TensorBoard()
                tb.configure(argv=[None, "--logdir", str(self._logdir)])
                tb.launch()
                self._tensor_board_dir = self._logdir

    def get_availability(self):
        with self._data_lock:
            return {
                "available": True,
                "trials_available": self._trials_available
            }

    def run(self):
        while True:
            with self._data_lock:
                self.collect()
            time.sleep(self._reload_interval)

    def collect_errors(self, df):
        sub_dirs = os.listdir(self._logdir)
        trial_names = filter(
            lambda d: os.path.isdir(os.path.join(self._logdir, d)), sub_dirs)
        for trial in trial_names:
            error_path = os.path.join(self._logdir, trial, "error.txt")
            if os.path.isfile(error_path):
                self._trials_available = True
                with open(error_path) as f:
                    text = f.read()
                    self._errors[str(trial)] = {
                        "text": text,
                        "job_id": os.path.basename(self._logdir),
                        "trial_id": "No Trial ID"
                    }
                    other_data = df[df["logdir"].str.contains(trial)]
                    if len(other_data) > 0:
                        trial_id = other_data["trial_id"].values[0]
                        self._errors[str(trial)]["trial_id"] = str(trial_id)
                        if str(trial_id) in self._trial_records.keys():
                            self._trial_records[str(trial_id)]["error"] = text
                            self._trial_records[str(trial_id)][
                                "status"] = "ERROR"

    def collect(self):
        """
        Collects and cleans data on the running Tune experiment from the
        Tune logs so that users can see this information in the front-end
        client
        """
        self._trial_records = {}
        self._errors = {}
        if not self._logdir:
            return

        # search through all the sub_directories in log directory
        analysis = Analysis(str(self._logdir))
        df = analysis.dataframe()

        if len(df) == 0 or "trial_id" not in df.columns:
            return

        self._trials_available = True

        # make sure that data will convert to JSON without error
        df["trial_id_key"] = df["trial_id"].astype(str)
        df = df.fillna(0)

        trial_ids = df["trial_id"]
        for i, value in df["trial_id"].iteritems():
            if type(value) != str and type(value) != int:
                trial_ids[i] = int(value)

        df["trial_id"] = trial_ids

        # convert df to python dict
        df = df.set_index("trial_id_key")
        trial_data = df.to_dict(orient="index")

        # clean data and update class attribute
        if len(trial_data) > 0:
            trial_data = self.clean_trials(trial_data)
            self._trial_records.update(trial_data)

        self.collect_errors(df)

    def clean_trials(self, trial_details):
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
            "experiment_tag", "trial_id"
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
                details[key] = round(details[key], 12)

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

            details["job_id"] = os.path.basename(self._logdir)
            details["error"] = "No Error"

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

    # TODO(sang): Add a URL validation.
    metrics_export_address = os.environ.get("METRICS_EXPORT_ADDRESS")

    try:
        dashboard = Dashboard(
            args.host,
            args.port,
            args.redis_address,
            args.temp_dir,
            redis_password=args.redis_password,
            metrics_export_address=metrics_export_address)
        dashboard.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The dashboard on node {} failed with the following "
                   "error:\n{}".format(platform.node(), traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.DASHBOARD_DIED_ERROR, message)
        if isinstance(e, OSError) and e.errno == errno.ENOENT:
            logger.warning(message)
        else:
            raise e
