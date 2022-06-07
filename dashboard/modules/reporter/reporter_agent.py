import asyncio
import datetime
import json
import logging
import os
import socket
import subprocess
import sys
import traceback
import warnings


import ray
import ray.dashboard.modules.reporter.reporter_consts as reporter_consts
from ray.dashboard import k8s_utils
import ray.dashboard.utils as dashboard_utils
import ray.experimental.internal_kv as internal_kv
from ray._private.gcs_pubsub import GcsAioPublisher
import ray._private.services
import ray._private.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.ray_constants import DEBUG_AUTOSCALING_STATUS
from ray._private.metrics_agent import MetricsAgent, Gauge, Record
from ray.util.debug import log_once
import psutil

logger = logging.getLogger(__name__)

enable_gpu_usage_check = True

# Are we in a K8s pod?
IN_KUBERNETES_POD = "KUBERNETES_SERVICE_HOST" in os.environ
# Flag to enable showing disk usage when running in a K8s pod,
# disk usage defined as the result of running psutil.disk_usage("/")
# in the Ray container.
ENABLE_K8S_DISK_USAGE = os.environ.get("RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE") == "1"

try:
    import gpustat.core as gpustat
except (ModuleNotFoundError, ImportError):
    gpustat = None
    if log_once("gpustat_import_warning"):
        warnings.warn(
            "`gpustat` package is not installed. GPU monitoring is "
            "not available. To have full functionality of the "
            "dashboard please install `pip install ray["
            "default]`.)"
        )


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


def jsonify_asdict(o) -> str:
    return json.dumps(dashboard_utils.to_google_style(recursive_asdict(o)))


# A list of gauges to record and export metrics.
METRICS_GAUGES = {
    "node_cpu_utilization": Gauge(
        "node_cpu_utilization", "Total CPU usage on a ray node", "percentage", ["ip"]
    ),
    "node_cpu_count": Gauge(
        "node_cpu_count", "Total CPUs available on a ray node", "cores", ["ip"]
    ),
    "node_mem_used": Gauge(
        "node_mem_used", "Memory usage on a ray node", "bytes", ["ip"]
    ),
    "node_mem_available": Gauge(
        "node_mem_available", "Memory available on a ray node", "bytes", ["ip"]
    ),
    "node_mem_total": Gauge(
        "node_mem_total", "Total memory on a ray node", "bytes", ["ip"]
    ),
    "node_gpus_available": Gauge(
        "node_gpus_available",
        "Total GPUs available on a ray node",
        "percentage",
        ["ip"],
    ),
    "node_gpus_utilization": Gauge(
        "node_gpus_utilization", "Total GPUs usage on a ray node", "percentage", ["ip"]
    ),
    "node_gram_used": Gauge(
        "node_gram_used", "Total GPU RAM usage on a ray node", "bytes", ["ip"]
    ),
    "node_gram_available": Gauge(
        "node_gram_available", "Total GPU RAM available on a ray node", "bytes", ["ip"]
    ),
    "node_disk_io_read": Gauge(
        "node_disk_io_read", "Total read from disk", "bytes", ["ip"]
    ),
    "node_disk_io_write": Gauge(
        "node_disk_io_write", "Total written to disk", "bytes", ["ip"]
    ),
    "node_disk_io_read_count": Gauge(
        "node_disk_io_read_count", "Total read ops from disk", "io", ["ip"]
    ),
    "node_disk_io_write_count": Gauge(
        "node_disk_io_write_count", "Total write ops to disk", "io", ["ip"]
    ),
    "node_disk_io_read_speed": Gauge(
        "node_disk_io_read_speed", "Disk read speed", "bytes/sec", ["ip"]
    ),
    "node_disk_io_write_speed": Gauge(
        "node_disk_io_write_speed", "Disk write speed", "bytes/sec", ["ip"]
    ),
    "node_disk_read_iops": Gauge(
        "node_disk_read_iops", "Disk read iops", "iops", ["ip"]
    ),
    "node_disk_write_iops": Gauge(
        "node_disk_write_iops", "Disk write iops", "iops", ["ip"]
    ),
    "node_disk_usage": Gauge(
        "node_disk_usage", "Total disk usage (bytes) on a ray node", "bytes", ["ip"]
    ),
    "node_disk_free": Gauge(
        "node_disk_free", "Total disk free (bytes) on a ray node", "bytes", ["ip"]
    ),
    "node_disk_utilization_percentage": Gauge(
        "node_disk_utilization_percentage",
        "Total disk utilization (percentage) on a ray node",
        "percentage",
        ["ip"],
    ),
    "node_network_sent": Gauge(
        "node_network_sent", "Total network sent", "bytes", ["ip"]
    ),
    "node_network_received": Gauge(
        "node_network_received", "Total network received", "bytes", ["ip"]
    ),
    "node_network_send_speed": Gauge(
        "node_network_send_speed", "Network send speed", "bytes/sec", ["ip"]
    ),
    "node_network_receive_speed": Gauge(
        "node_network_receive_speed", "Network receive speed", "bytes/sec", ["ip"]
    ),
    "raylet_cpu": Gauge(
        "raylet_cpu", "CPU usage of the raylet on a node.", "percentage", ["ip", "pid"]
    ),
    "raylet_mem": Gauge(
        "raylet_mem",
        "RSS usage of the Raylet on the node.",
        "MB",
        ["ip", "pid"],
    ),
    "raylet_mem_uss": Gauge(
        "raylet_mem_uss",
        "USS usage of the Raylet on the node. Only available on Linux",
        "MB",
        ["ip", "pid"],
    ),
    "workers_cpu": Gauge(
        "workers_cpu",
        "Total CPU usage of all workers on a node.",
        "percentage",
        ["ip"],
    ),
    "workers_mem": Gauge(
        "workers_mem",
        "RSS usage of all workers on the node.",
        "MB",
        ["ip"],
    ),
    "workers_mem_uss": Gauge(
        "workers_mem_uss",
        "USS usage of all workers on the node. Only available on Linux",
        "MB",
        ["ip"],
    ),
    "cluster_active_nodes": Gauge(
        "cluster_active_nodes", "Active nodes on the cluster", "count", ["node_type"]
    ),
    "cluster_failed_nodes": Gauge(
        "cluster_failed_nodes", "Failed nodes on the cluster", "count", ["node_type"]
    ),
    "cluster_pending_nodes": Gauge(
        "cluster_pending_nodes", "Pending nodes on the cluster", "count", ["node_type"]
    ),
}


class ReporterAgent(
    dashboard_utils.DashboardAgentModule, reporter_pb2_grpc.ReporterServiceServicer
):
    """A monitor process for monitoring Ray nodes.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config
    """

    def __init__(self, dashboard_agent):
        """Initialize the reporter object."""
        super().__init__(dashboard_agent)
        if IN_KUBERNETES_POD:
            # psutil does not compute this correctly when in a K8s pod.
            # Use ray._private.utils instead.
            cpu_count = ray._private.utils.get_num_cpus()
            self._cpu_counts = (cpu_count, cpu_count)
        else:
            self._cpu_counts = (psutil.cpu_count(), psutil.cpu_count(logical=False))

        self._ip = dashboard_agent.ip
        self._is_head_node = self._ip == dashboard_agent.gcs_address.split(":")[0]
        self._hostname = socket.gethostname()
        self._workers = set()
        self._network_stats_hist = [(0, (0.0, 0.0))]  # time, (sent, recv)
        self._disk_io_stats_hist = [
            (0, (0.0, 0.0, 0, 0))
        ]  # time, (bytes read, bytes written, read ops, write ops)
        self._metrics_collection_disabled = dashboard_agent.metrics_collection_disabled
        self._metrics_agent = None
        if not self._metrics_collection_disabled:
            self._metrics_agent = MetricsAgent(
                "127.0.0.1" if self._ip == "127.0.0.1" else "",
                dashboard_agent.metrics_export_port,
            )
        self._key = (
            f"{reporter_consts.REPORTER_PREFIX}" f"{self._dashboard_agent.node_id}"
        )

    async def GetProfilingStats(self, request, context):
        pid = request.pid
        duration = request.duration
        profiling_file_path = os.path.join(
            ray._private.utils.get_ray_temp_dir(), f"{pid}_profiling.txt"
        )
        sudo = "sudo" if ray._private.utils.get_user() != "root" else ""
        process = await asyncio.create_subprocess_shell(
            f"{sudo} $(which py-spy) record "
            f"-o {profiling_file_path} -p {pid} -d {duration} -f speedscope",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            profiling_stats = ""
        else:
            with open(profiling_file_path, "r") as f:
                profiling_stats = f.read()
        return reporter_pb2.GetProfilingStatsReply(
            profiling_stats=profiling_stats, std_out=stdout, std_err=stderr
        )

    async def ReportOCMetrics(self, request, context):
        # Do nothing if metrics collection is disabled.
        if self._metrics_collection_disabled:
            return reporter_pb2.ReportOCMetricsReply()

        # This function receives a GRPC containing OpenCensus (OC) metrics
        # from a Ray process, then exposes those metrics to Prometheus.
        try:
            self._metrics_agent.record_metric_points_from_protobuf(request.metrics)
        except Exception:
            logger.error(traceback.format_exc())
        return reporter_pb2.ReportOCMetricsReply()

    @staticmethod
    def _get_cpu_percent():
        if IN_KUBERNETES_POD:
            return k8s_utils.cpu_percent()
        else:
            return psutil.cpu_percent()

    @staticmethod
    def _get_gpu_usage():
        global enable_gpu_usage_check
        if gpustat is None or not enable_gpu_usage_check:
            return []
        gpu_utilizations = []
        gpus = []
        try:
            gpus = gpustat.new_query().gpus
        except Exception as e:
            logger.debug(f"gpustat failed to retrieve GPU information: {e}")

            # gpustat calls pynvml.nvmlInit()
            # On machines without GPUs, this can run subprocesses that spew to
            # stderr. Then with log_to_driver=True, we get log spew from every
            # single raylet. To avoid this, disable the GPU usage check on
            # certain errors.
            # https://github.com/ray-project/ray/issues/14305
            # https://github.com/ray-project/ray/pull/21686
            if type(e).__name__ == "NVMLError_DriverNotLoaded":
                enable_gpu_usage_check = False

        for gpu in gpus:
            # Note the keys in this dict have periods which throws
            # off javascript so we change .s to _s
            gpu_data = {"_".join(key.split(".")): val for key, val in gpu.entry.items()}
            gpu_utilizations.append(gpu_data)
        return gpu_utilizations

    @staticmethod
    def _get_boot_time():
        if IN_KUBERNETES_POD:
            # Return start time of container entrypoint
            return psutil.Process(pid=1).create_time()
        else:
            return psutil.boot_time()

    @staticmethod
    def _get_network_stats():
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items() if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def _get_mem_usage():
        total = ray._private.utils.get_system_memory()
        used = ray._private.utils.get_used_memory()
        available = total - used
        percent = round(used / total, 3) * 100
        return total, available, percent, used

    @staticmethod
    def _get_disk_usage():
        if IN_KUBERNETES_POD and not ENABLE_K8S_DISK_USAGE:
            # If in a K8s pod, disable disk display by passing in dummy values.
            return {
                "/": psutil._common.sdiskusage(total=1, used=0, free=1, percent=0.0)
            }
        if sys.platform == "win32":
            root = psutil.disk_partitions()[0].mountpoint
        else:
            root = os.sep
        tmp = ray._private.utils.get_user_temp_dir()
        return {
            "/": psutil.disk_usage(root),
            tmp: psutil.disk_usage(tmp),
        }

    @staticmethod
    def _get_disk_io_stats():
        stats = psutil.disk_io_counters()
        return (
            stats.read_bytes,
            stats.write_bytes,
            stats.read_count,
            stats.write_count,
        )

    def _get_workers(self):
        raylet_proc = self._get_raylet_proc()
        if raylet_proc is None:
            return []
        else:
            workers = set(raylet_proc.children())
            # Remove the current process (reporter agent), which is also a child of
            # the Raylet.
            workers.discard(psutil.Process())
            self._workers = workers
            return [
                w.as_dict(
                    attrs=[
                        "pid",
                        "create_time",
                        "cpu_percent",
                        "cpu_times",
                        "cmdline",
                        "memory_info",
                        "memory_full_info",
                    ]
                )
                for w in self._workers
                if w.status() != psutil.STATUS_ZOMBIE
            ]

    @staticmethod
    def _get_raylet_proc():
        try:
            curr_proc = psutil.Process()
            # Here, parent is always raylet because the
            # dashboard agent is a child of the raylet process.
            parent = curr_proc.parent()
            if parent is not None:
                if parent.pid == 1:
                    return None
                if parent.status() == psutil.STATUS_ZOMBIE:
                    return None
            return parent
        except (psutil.AccessDenied, ProcessLookupError):
            pass
        return None

    def _get_raylet(self):
        raylet_proc = self._get_raylet_proc()
        if raylet_proc is None:
            return {}
        else:
            return raylet_proc.as_dict(
                attrs=[
                    "pid",
                    "create_time",
                    "cpu_percent",
                    "cpu_times",
                    "cmdline",
                    "memory_info",
                    "memory_full_info",
                ]
            )

    def _get_load_avg(self):
        if sys.platform == "win32":
            cpu_percent = psutil.cpu_percent()
            load = (cpu_percent, cpu_percent, cpu_percent)
        else:
            load = os.getloadavg()
        per_cpu_load = tuple((round(x / self._cpu_counts[0], 2) for x in load))
        return load, per_cpu_load

    @staticmethod
    def _compute_speed_from_hist(hist):
        while len(hist) > 7:
            hist.pop(0)
        then, prev_stats = hist[0]
        now, now_stats = hist[-1]
        time_delta = now - then
        return tuple((y - x) / time_delta for x, y in zip(prev_stats, now_stats))

    def _get_all_stats(self):
        now = dashboard_utils.to_posix_time(datetime.datetime.utcnow())
        network_stats = self._get_network_stats()
        self._network_stats_hist.append((now, network_stats))
        network_speed_stats = self._compute_speed_from_hist(self._network_stats_hist)

        disk_stats = self._get_disk_io_stats()
        self._disk_io_stats_hist.append((now, disk_stats))
        disk_speed_stats = self._compute_speed_from_hist(self._disk_io_stats_hist)

        return {
            "now": now,
            "hostname": self._hostname,
            "ip": self._ip,
            "cpu": self._get_cpu_percent(),
            "cpus": self._cpu_counts,
            "mem": self._get_mem_usage(),
            "workers": self._get_workers(),
            "raylet": self._get_raylet(),
            "bootTime": self._get_boot_time(),
            "loadAvg": self._get_load_avg(),
            "disk": self._get_disk_usage(),
            "disk_io": disk_stats,
            "disk_io_speed": disk_speed_stats,
            "gpus": self._get_gpu_usage(),
            "network": network_stats,
            "network_speed": network_speed_stats,
            # Deprecated field, should be removed with frontend.
            "cmdline": self._get_raylet().get("cmdline", []),
        }

    def _record_stats(self, stats, cluster_stats):
        records_reported = []
        ip = stats["ip"]

        # -- Instance count of cluster --
        # Only report cluster stats on head node
        if "autoscaler_report" in cluster_stats and self._is_head_node:
            active_nodes = cluster_stats["autoscaler_report"]["active_nodes"]
            for node_type, active_node_count in active_nodes.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_active_nodes"],
                        value=active_node_count,
                        tags={"node_type": node_type},
                    )
                )

            failed_nodes = cluster_stats["autoscaler_report"]["failed_nodes"]
            failed_nodes_dict = {}
            for node_ip, node_type in failed_nodes:
                if node_type in failed_nodes_dict:
                    failed_nodes_dict[node_type] += 1
                else:
                    failed_nodes_dict[node_type] = 1

            for node_type, failed_node_count in failed_nodes_dict.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_failed_nodes"],
                        value=failed_node_count,
                        tags={"node_type": node_type},
                    )
                )

            pending_nodes = cluster_stats["autoscaler_report"]["pending_nodes"]
            pending_nodes_dict = {}
            for node_ip, node_type, status_message in pending_nodes:
                if node_type in pending_nodes_dict:
                    pending_nodes_dict[node_type] += 1
                else:
                    pending_nodes_dict[node_type] = 1

            for node_type, pending_node_count in pending_nodes_dict.items():
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["cluster_pending_nodes"],
                        value=pending_node_count,
                        tags={"node_type": node_type},
                    )
                )

        # -- CPU per node --
        cpu_usage = float(stats["cpu"])
        cpu_record = Record(
            gauge=METRICS_GAUGES["node_cpu_utilization"],
            value=cpu_usage,
            tags={"ip": ip},
        )

        cpu_count, _ = stats["cpus"]
        cpu_count_record = Record(
            gauge=METRICS_GAUGES["node_cpu_count"], value=cpu_count, tags={"ip": ip}
        )

        # -- Mem per node --
        mem_total, mem_available, _, mem_used = stats["mem"]
        mem_used_record = Record(
            gauge=METRICS_GAUGES["node_mem_used"], value=mem_used, tags={"ip": ip}
        )
        mem_available_record = Record(
            gauge=METRICS_GAUGES["node_mem_available"],
            value=mem_available,
            tags={"ip": ip},
        )
        mem_total_record = Record(
            gauge=METRICS_GAUGES["node_mem_total"], value=mem_total, tags={"ip": ip}
        )

        # -- GPU per node --
        gpus = stats["gpus"]
        gpus_available = len(gpus)

        if gpus_available:
            gpus_utilization, gram_used, gram_total = 0, 0, 0
            for gpu in gpus:
                # Consume GPU may not report its utilization.
                if gpu["utilization_gpu"] is not None:
                    gpus_utilization += gpu["utilization_gpu"]
                gram_used += gpu["memory_used"]
                gram_total += gpu["memory_total"]

            gram_available = gram_total - gram_used

            gpus_available_record = Record(
                gauge=METRICS_GAUGES["node_gpus_available"],
                value=gpus_available,
                tags={"ip": ip},
            )
            gpus_utilization_record = Record(
                gauge=METRICS_GAUGES["node_gpus_utilization"],
                value=gpus_utilization,
                tags={"ip": ip},
            )
            gram_used_record = Record(
                gauge=METRICS_GAUGES["node_gram_used"], value=gram_used, tags={"ip": ip}
            )
            gram_available_record = Record(
                gauge=METRICS_GAUGES["node_gram_available"],
                value=gram_available,
                tags={"ip": ip},
            )
            records_reported.extend(
                [
                    gpus_available_record,
                    gpus_utilization_record,
                    gram_used_record,
                    gram_available_record,
                ]
            )

        # -- Disk per node --
        disk_io_stats = stats["disk_io"]
        disk_read_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_read"],
            value=disk_io_stats[0],
            tags={"ip": ip},
        )
        disk_write_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_write"],
            value=disk_io_stats[1],
            tags={"ip": ip},
        )
        disk_read_count_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_read_count"],
            value=disk_io_stats[2],
            tags={"ip": ip},
        )
        disk_write_count_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_write_count"],
            value=disk_io_stats[3],
            tags={"ip": ip},
        )
        disk_io_speed_stats = stats["disk_io_speed"]
        disk_read_speed_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_read_speed"],
            value=disk_io_speed_stats[0],
            tags={"ip": ip},
        )
        disk_write_speed_record = Record(
            gauge=METRICS_GAUGES["node_disk_io_write_speed"],
            value=disk_io_speed_stats[1],
            tags={"ip": ip},
        )
        disk_read_iops_record = Record(
            gauge=METRICS_GAUGES["node_disk_read_iops"],
            value=disk_io_speed_stats[2],
            tags={"ip": ip},
        )
        disk_write_iops_record = Record(
            gauge=METRICS_GAUGES["node_disk_write_iops"],
            value=disk_io_speed_stats[3],
            tags={"ip": ip},
        )
        used, free = 0, 0
        for entry in stats["disk"].values():
            used += entry.used
            free += entry.free
        disk_utilization = float(used / (used + free)) * 100
        disk_usage_record = Record(
            gauge=METRICS_GAUGES["node_disk_usage"], value=used, tags={"ip": ip}
        )
        disk_free_record = Record(
            gauge=METRICS_GAUGES["node_disk_free"], value=free, tags={"ip": ip}
        )
        disk_utilization_percentage_record = Record(
            gauge=METRICS_GAUGES["node_disk_utilization_percentage"],
            value=disk_utilization,
            tags={"ip": ip},
        )

        # -- Network speed (send/receive) stats per node --
        network_stats = stats["network"]
        network_sent_record = Record(
            gauge=METRICS_GAUGES["node_network_sent"],
            value=network_stats[0],
            tags={"ip": ip},
        )
        network_received_record = Record(
            gauge=METRICS_GAUGES["node_network_received"],
            value=network_stats[1],
            tags={"ip": ip},
        )

        # -- Network speed (send/receive) per node --
        network_speed_stats = stats["network_speed"]
        network_send_speed_record = Record(
            gauge=METRICS_GAUGES["node_network_send_speed"],
            value=network_speed_stats[0],
            tags={"ip": ip},
        )
        network_receive_speed_record = Record(
            gauge=METRICS_GAUGES["node_network_receive_speed"],
            value=network_speed_stats[1],
            tags={"ip": ip},
        )

        raylet_stats = stats["raylet"]
        if raylet_stats:
            raylet_pid = str(raylet_stats["pid"])
            # -- raylet CPU --
            raylet_cpu_usage = float(raylet_stats["cpu_percent"]) * 100
            records_reported.append(
                Record(
                    gauge=METRICS_GAUGES["raylet_cpu"],
                    value=raylet_cpu_usage,
                    tags={"ip": ip, "pid": raylet_pid},
                )
            )

            # -- raylet mem --
            raylet_rss = float(raylet_stats["memory_info"].rss) / 1.0e6
            records_reported.append(
                Record(
                    gauge=METRICS_GAUGES["raylet_mem"],
                    value=raylet_rss,
                    tags={"ip": ip, "pid": raylet_pid},
                )
            )
            raylet_mem_full_info = raylet_stats.get("memory_full_info")
            if raylet_mem_full_info is not None:
                raylet_uss = float(raylet_mem_full_info.uss) / 1.0e6
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["raylet_mem_uss"],
                        value=raylet_uss,
                        tags={"ip": ip, "pid": raylet_pid},
                    )
                )

        workers_stats = stats["workers"]
        if workers_stats:
            total_workers_cpu_percentage = 0.0
            total_workers_rss = 0.0
            total_workers_uss = 0.0
            for worker in workers_stats:
                total_workers_cpu_percentage += float(worker["cpu_percent"]) * 100.0
                total_workers_rss += float(worker["memory_info"].rss) / 1.0e6
                worker_mem_full_info = worker.get("memory_full_info")
                if worker_mem_full_info is not None:
                    total_workers_uss += float(worker_mem_full_info.uss) / 1.0e6

            records_reported.append(
                Record(
                    gauge=METRICS_GAUGES["workers_cpu"],
                    value=total_workers_cpu_percentage,
                    tags={"ip": ip},
                )
            )

            records_reported.append(
                Record(
                    gauge=METRICS_GAUGES["workers_mem"],
                    value=total_workers_rss,
                    tags={"ip": ip},
                )
            )
            if total_workers_uss > 0.0:
                records_reported.append(
                    Record(
                        gauge=METRICS_GAUGES["workers_mem_uss"],
                        value=total_workers_uss,
                        tags={"ip": ip},
                    )
                )

        records_reported.extend(
            [
                cpu_record,
                cpu_count_record,
                mem_used_record,
                mem_available_record,
                mem_total_record,
                disk_read_record,
                disk_write_record,
                disk_read_count_record,
                disk_write_count_record,
                disk_read_speed_record,
                disk_write_speed_record,
                disk_read_iops_record,
                disk_write_iops_record,
                disk_usage_record,
                disk_free_record,
                disk_utilization_percentage_record,
                network_sent_record,
                network_received_record,
                network_send_speed_record,
                network_receive_speed_record,
            ]
        )
        return records_reported

    async def _perform_iteration(self, publisher):
        """Get any changes to the log files and push updates to kv."""
        while True:
            try:
                formatted_status_string = internal_kv._internal_kv_get(
                    DEBUG_AUTOSCALING_STATUS
                )

                stats = self._get_all_stats()
                # Report stats only when metrics collection is enabled.
                if not self._metrics_collection_disabled:
                    cluster_stats = (
                        json.loads(formatted_status_string.decode())
                        if formatted_status_string
                        else {}
                    )
                    records_reported = self._record_stats(stats, cluster_stats)
                    self._metrics_agent.record_reporter_stats(records_reported)
                await publisher.publish_resource_usage(self._key, jsonify_asdict(stats))

            except Exception:
                logger.exception("Error publishing node physical stats.")
            await asyncio.sleep(reporter_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)

    async def run(self, server):
        if server:
            reporter_pb2_grpc.add_ReporterServiceServicer_to_server(self, server)

        gcs_addr = self._dashboard_agent.gcs_address
        assert gcs_addr is not None
        publisher = GcsAioPublisher(address=gcs_addr)
        await self._perform_iteration(publisher)

    @staticmethod
    def is_minimal_module():
        return False
