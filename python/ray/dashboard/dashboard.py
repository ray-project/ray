from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import aiohttp.web
except ImportError:
    print("The dashboard requires aiohttp to run.")
    import sys
    sys.exit(1)

import argparse
import copy
import datetime
import json
import logging
import os
import re
import threading
import time
import traceback
import yaml

from pathlib import Path
from collections import Counter
from collections import defaultdict
from operator import itemgetter
from typing import Dict

import grpc
from google.protobuf.json_format import MessageToDict
import ray
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
import ray.ray_constants as ray_constants
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


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
                with open(
                        Path("~/ray_bootstrap_config.yaml").expanduser()) as f:
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
            return await json_response(result=D)

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

        static_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "client/build/static")
        if not os.path.isdir(static_dir):
            raise ValueError(
                "Dashboard static asset directory not found at '{}'. If "
                "installing from source, please follow the additional steps "
                "required to build the dashboard.".format(static_dir))
        self.app.router.add_static("/static", static_dir)

        self.app.router.add_get("/api/ray_config", ray_config)
        self.app.router.add_get("/api/node_info", node_info)
        self.app.router.add_get("/api/raylet_info", raylet_info)
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
        aiohttp.web.run_app(self.app, host=self.host, port=self.port)


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._node_stats_lock = threading.Lock()

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.init(redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def calculate_totals(self) -> Dict:
        total_boot_time = 0
        total_cpus = 0
        total_workers = 0
        total_load = [0.0, 0.0, 0.0]
        total_storage_avail = 0
        total_storage_total = 0
        total_ram_avail = 0
        total_ram_total = 0
        total_sent = 0
        total_recv = 0

        for v in self._node_stats.values():
            total_boot_time += v["boot_time"]
            total_cpus += v["cpus"][0]
            total_workers += len(v["workers"])
            total_load[0] += v["load_avg"][0][0]
            total_load[1] += v["load_avg"][0][1]
            total_load[2] += v["load_avg"][0][2]
            total_storage_avail += v["disk"]["/"]["free"]
            total_storage_total += v["disk"]["/"]["total"]
            total_ram_avail += v["mem"][1]
            total_ram_total += v["mem"][0]
            total_sent += v["net"][0]
            total_recv += v["net"][1]

        return {
            "boot_time": total_boot_time,
            "n_workers": total_workers,
            "n_cores": total_cpus,
            "m_avail": total_ram_avail,
            "m_total": total_ram_total,
            "d_avail": total_storage_avail,
            "d_total": total_storage_total,
            "load": total_load,
            "n_sent": total_sent,
            "n_recv": total_recv,
        }

    def calculate_tasks(self) -> Counter:
        return Counter(
            (x["name"]
             for y in (v["workers"] for v in self._node_stats.values())
             for x in y))

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
                "totals": self.calculate_totals(),
                "tasks": self.calculate_tasks(),
                "clients": node_stats,
                "log_counts": self.calculate_log_counts(),
                "error_counts": self.calculate_error_counts(),
            }

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

        self._raylet_stats_lock = threading.Lock()
        self._raylet_stats = {}

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

            # Now add node connections of new nodes.
            for node in self.nodes:
                node_id = node["NodeID"]
                if node_id not in self.stubs:
                    channel = grpc.insecure_channel("{}:{}".format(
                        node["NodeManagerAddress"], node["NodeManagerPort"]))
                    stub = node_manager_pb2_grpc.NodeManagerServiceStub(
                        channel)
                    self.stubs[node_id] = stub

    def get_raylet_stats(self) -> Dict:
        with self._raylet_stats_lock:
            return copy.deepcopy(self._raylet_stats)

    def run(self):
        counter = 0
        while True:
            time.sleep(1.0)
            replies = {}
            for node in self.nodes:
                node_id = node["NodeID"]
                stub = self.stubs[node_id]
                reply = stub.GetNodeStats(node_manager_pb2.NodeStatsRequest())
                replies[node["NodeManagerAddress"]] = reply
            with self._raylet_stats_lock:
                for address, reply in replies.items():
                    self._raylet_stats[address] = MessageToDict(reply)
            counter += 1
            # From time to time, check if new nodes have joined the cluster
            # and update self.nodes
            if counter % 10:
                self.update_nodes()


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
        raise e
