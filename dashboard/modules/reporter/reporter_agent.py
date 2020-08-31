import asyncio
import datetime
import json
import logging
import os
import socket
import subprocess
import sys
import traceback

import aioredis

import ray
import ray.gcs_utils
import ray.new_dashboard.modules.reporter.reporter_consts as reporter_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.services
import ray.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.metrics_agent import MetricsAgent
import psutil

logger = logging.getLogger(__name__)

try:
    import gpustat.core as gpustat
except ImportError:
    gpustat = None
    logger.warning(
        "Install gpustat with 'pip install gpustat' to enable GPU monitoring.")


def recursive_asdict(o):
    if isinstance(o, tuple) and hasattr(o, "_asdict"):
        return recursive_asdict(o._asdict())

    if isinstance(o, (tuple, list)):
        L = []
        for k in o:
            L.append(recursive_asdict(k))
        return L

    if isinstance(o, dict):
        D = {k: recursive_asdict(v) for k, v in o.items()}
        return D

    return o


def jsonify_asdict(o):
    return json.dumps(dashboard_utils.to_google_style(recursive_asdict(o)))


class ReporterAgent(dashboard_utils.DashboardAgentModule,
                    reporter_pb2_grpc.ReporterServiceServicer):
    """A monitor process for monitoring Ray nodes.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config
    """

    def __init__(self, dashboard_agent):
        """Initialize the reporter object."""
        super().__init__(dashboard_agent)
        self._cpu_counts = (psutil.cpu_count(),
                            psutil.cpu_count(logical=False))
        self._ip = ray.services.get_node_ip_address()
        self._hostname = socket.gethostname()
        self._workers = set()
        self._network_stats_hist = [(0, (0.0, 0.0))]  # time, (sent, recv)
        self._metrics_agent = MetricsAgent(dashboard_agent.metrics_export_port)

    async def GetProfilingStats(self, request, context):
        pid = request.pid
        duration = request.duration
        profiling_file_path = os.path.join(ray.utils.get_ray_temp_dir(),
                                           f"{pid}_profiling.txt")
        sudo = "sudo" if ray.utils.get_user() != "root" else ""
        process = await asyncio.create_subprocess_shell(
            f"{sudo} $(which py-spy) record "
            f"-o {profiling_file_path} -p {pid} -d {duration} -f speedscope",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            profiling_stats = ""
        else:
            with open(profiling_file_path, "r") as f:
                profiling_stats = f.read()
        return reporter_pb2.GetProfilingStatsReply(
            profiling_stats=profiling_stats, std_out=stdout, std_err=stderr)

    async def ReportMetrics(self, request, context):
        # NOTE: Exceptions are not propagated properly
        # when we don't catch them here.
        try:
            metrcs_description_required = (
                self._metrics_agent.record_metrics_points(
                    request.metrics_points))
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())

        # If metrics description is missing, we should notify cpp processes
        # that we need them. Cpp processes will then report them to here.
        # We need it when (1) a new metric is reported (application metric)
        # (2) a reporter goes down and restarted (currently not implemented).
        return reporter_pb2.ReportMetricsReply(
            metrcs_description_required=metrcs_description_required)

    @staticmethod
    def _get_cpu_percent():
        return psutil.cpu_percent()

    @staticmethod
    def _get_gpu_usage():
        if gpustat is None:
            return []
        gpu_utilizations = []
        gpus = []
        try:
            gpus = gpustat.new_query().gpus
        except Exception as e:
            logger.debug(
                "gpustat failed to retrieve GPU information: {}".format(e))
        for gpu in gpus:
            # Note the keys in this dict have periods which throws
            # off javascript so we change .s to _s
            gpu_data = {
                "_".join(key.split(".")): val
                for key, val in gpu.entry.items()
            }
            gpu_utilizations.append(gpu_data)
        return gpu_utilizations

    @staticmethod
    def _get_boot_time():
        return psutil.boot_time()

    @staticmethod
    def _get_network_stats():
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def _get_mem_usage():
        vm = psutil.virtual_memory()
        return vm.total, vm.available, vm.percent

    @staticmethod
    def _get_disk_usage():
        dirs = [
            os.environ["USERPROFILE"] if sys.platform == "win32" else os.sep,
            ray.utils.get_user_temp_dir(),
        ]
        return {x: psutil.disk_usage(x) for x in dirs}

    def _get_workers(self):
        curr_proc = psutil.Process()
        parent = curr_proc.parent()
        if parent is None or parent.pid == 1:
            return []
        else:
            workers = set(parent.children())
            self._workers.intersection_update(workers)
            self._workers.update(workers)
            self._workers.discard(curr_proc)
            return [
                w.as_dict(attrs=[
                    "pid",
                    "create_time",
                    "cpu_percent",
                    "cpu_times",
                    "cmdline",
                    "memory_info",
                ]) for w in self._workers if w.status() != psutil.STATUS_ZOMBIE
            ]

    @staticmethod
    def _get_raylet_cmdline():
        curr_proc = psutil.Process()
        parent = curr_proc.parent()
        if parent.pid == 1:
            return ""
        else:
            return parent.cmdline()

    def _get_load_avg(self):
        if sys.platform == "win32":
            cpu_percent = psutil.cpu_percent()
            load = (cpu_percent, cpu_percent, cpu_percent)
        else:
            load = os.getloadavg()
        per_cpu_load = tuple((round(x / self._cpu_counts[0], 2) for x in load))
        return load, per_cpu_load

    def _get_all_stats(self):
        now = dashboard_utils.to_posix_time(datetime.datetime.utcnow())
        network_stats = self._get_network_stats()

        self._network_stats_hist.append((now, network_stats))
        self._network_stats_hist = self._network_stats_hist[-7:]
        then, prev_network_stats = self._network_stats_hist[0]
        netstats = ((network_stats[0] - prev_network_stats[0]) / (now - then),
                    (network_stats[1] - prev_network_stats[1]) / (now - then))

        return {
            "now": now,
            "hostname": self._hostname,
            "ip": self._ip,
            "cpu": self._get_cpu_percent(),
            "cpus": self._cpu_counts,
            "mem": self._get_mem_usage(),
            "workers": self._get_workers(),
            "bootTime": self._get_boot_time(),
            "loadAvg": self._get_load_avg(),
            "disk": self._get_disk_usage(),
            "gpus": self._get_gpu_usage(),
            "net": netstats,
            "cmdline": self._get_raylet_cmdline(),
        }

    async def _perform_iteration(self):
        """Get any changes to the log files and push updates to Redis."""
        aioredis_client = await aioredis.create_redis_pool(
            address=self._dashboard_agent.redis_address,
            password=self._dashboard_agent.redis_password)

        while True:
            try:
                stats = self._get_all_stats()
                await aioredis_client.publish(
                    "{}{}".format(reporter_consts.REPORTER_PREFIX,
                                  self._hostname), jsonify_asdict(stats))
            except Exception:
                logger.exception("Error publishing node physical stats.")
            await asyncio.sleep(
                reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)

    async def run(self, server):
        reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)
        await self._perform_iteration()
