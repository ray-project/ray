from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import aiohttp.web
except ModuleNotFoundError:
    print("The reporter requires aiohttp to run.")
    import sys
    sys.exit(1)

import argparse
import datetime
import json
import logging
import os
import threading
import traceback
import yaml

from pathlib import Path
from collections import Counter
from operator import itemgetter
from typing import Dict

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
                 redis_address,
                 http_port,
                 token,
                 temp_dir,
                 redis_password=None):
        """Initialize the dashboard object."""
        self.ip = ray.services.get_node_ip_address()
        self.port = http_port
        self.token = token
        self.temp_dir = temp_dir
        self.node_stats = NodeStats(redis_address, redis_password)

        self.app = aiohttp.web.Application(middlewares=[self.auth_middleware])
        self.setup_routes()

    @aiohttp.web.middleware
    async def auth_middleware(self, req, handler):
        def valid_token(req):
            # If the cookie token is correct, accept that.
            try:
                if req.cookies["token"] == self.token:
                    return True
            except KeyError:
                pass

            # If the query token is correct, accept that.
            try:
                if req.query["token"] == self.token:
                    return True
            except KeyError:
                pass

            # Reject.
            logger.warning("Dashboard: rejected an invalid token")
            return False

        # Check that the token is present, either in query or as cookie.
        if not valid_token(req):
            return aiohttp.web.Response(status=401, text="401 Unauthorized")

        resp = await handler(req)
        resp.cookies["token"] = self.token
        return resp

    def setup_routes(self):
        def forbidden() -> aiohttp.web.Response:
            return aiohttp.web.Response(status=403, text="403 Forbidden")

        def get_forbidden(_) -> aiohttp.web.Response:
            return forbidden()

        async def get_index(req) -> aiohttp.web.Response:
            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "index.html"))

        async def get_resource(req) -> aiohttp.web.Response:
            try:
                path = req.match_info["x"]
            except KeyError:
                return forbidden()

            if path not in ["main.css", "main.js"]:
                return forbidden()

            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "res/{}".format(path)))

        async def json_response(result=None, error=None,
                                ts=None) -> aiohttp.web.Response:
            if ts is None:
                ts = datetime.datetime.utcnow()

            return aiohttp.web.json_response({
                "result": result,
                "timestamp": to_unix_time(ts),
                "error": error,
            })

        async def ray_config(_) -> aiohttp.web.Response:
            try:
                with open(
                        Path("~/ray_bootstrap_config.yaml").expanduser()) as f:
                    cfg = yaml.load(f)
            except Exception:
                return await json_response(error="No config")

            D = {
                "min_workers": cfg["min_workers"],
                "max_workers": cfg["max_workers"],
                "initial_workers": cfg["initial_workers"],
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

        self.app.router.add_get("/", get_index)
        self.app.router.add_get("/index.html", get_index)
        self.app.router.add_get("/index.htm", get_index)
        self.app.router.add_get("/res/{x}", get_resource)

        self.app.router.add_get("/api/node_info", node_info)
        self.app.router.add_get("/api/super_client_table", node_info)
        self.app.router.add_get("/api/ray_config", ray_config)

        self.app.router.add_get("/{_}", get_forbidden)

    def log_dashboard_url(self):
        url = "http://{}:{}?token={}".format(self.ip, self.port, self.token)
        with open(os.path.join(self.temp_dir, "dashboard_url"), "w") as f:
            f.write(url)
        logger.info("Dashboard running on {}".format(url))

    def run(self):
        self.log_dashboard_url()
        self.node_stats.start()
        aiohttp.web.run_app(self.app, host="0.0.0.0", port=self.port)


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._node_stats_lock = threading.Lock()
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
            }

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(self.redis_key)
        logger.info("NodeStats: subscribed to {}".format(self.redis_key))

        for x in p.listen():
            try:
                D = json.loads(x["data"])
                with self._node_stats_lock:
                    self._node_stats[D["hostname"]] = D
            except Exception:
                logger.exception(traceback.format_exc())
                continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "dashboard to connect to."))
    parser.add_argument(
        "--http-port",
        required=True,
        type=int,
        help="The port to use for the HTTP server.")
    parser.add_argument(
        "--token",
        required=True,
        type=str,
        help="The token to use for the HTTP server.")
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

    dashboard = Dashboard(
        args.redis_address,
        args.http_port,
        args.token,
        args.temp_dir,
        redis_password=args.redis_password,
    )

    try:
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
