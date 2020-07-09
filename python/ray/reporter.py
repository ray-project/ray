import argparse
import logging
import json
import os
import traceback
import time
import datetime
import grpc
import platform
import subprocess
import sys
from concurrent import futures
import ray
import psutil
import ray.ray_constants as ray_constants
import ray.services
import ray.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

try:
    import gpustat.core as gpustat
except ImportError:
    gpustat = None
    logger.warning(
        "Install gpustat with 'pip install gpustat' to enable GPU monitoring.")


class ReporterServer(reporter_pb2_grpc.ReporterServiceServicer):
    def __init__(self):
        pass

    def GetProfilingStats(self, request, context):
        pid = request.pid
        duration = request.duration
        profiling_file_path = os.path.join(ray.utils.get_ray_temp_dir(),
                                           "{}_profiling.txt".format(pid))
        process = subprocess.Popen(
            "sudo $(which py-spy) record -o {} -p {} -d {} -f speedscope"
            .format(profiling_file_path, pid, duration),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            profiling_stats = ""
        else:
            with open(profiling_file_path, "r") as f:
                profiling_stats = f.read()
        return reporter_pb2.GetProfilingStatsReply(
            profiling_stats=profiling_stats, stdout=stdout, stderr=stderr)


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
    return json.dumps(recursive_asdict(o))


def is_worker(cmdline):
    return cmdline and cmdline[0].startswith("ray::")


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


class Reporter:
    """A monitor process for monitoring Ray nodes.

    Attributes:
        host (str): The hostname of this machine. Used to improve the log
            messages published to Redis.
        redis_client: A client used to communicate with the Redis server.
    """

    def __init__(self, redis_address, redis_password=None):
        """Initialize the reporter object."""
        self.cpu_counts = (psutil.cpu_count(), psutil.cpu_count(logical=False))
        self.ip = ray.services.get_node_ip_address()
        self.hostname = platform.node()

        _ = psutil.cpu_percent()  # For initialization

        self.redis_key = "{}.{}".format(ray.gcs_utils.REPORTER_CHANNEL,
                                        self.hostname)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self.network_stats_hist = [(0, (0.0, 0.0))]  # time, (sent, recv)

    @staticmethod
    def get_cpu_percent():
        return psutil.cpu_percent()

    @staticmethod
    def get_gpu_usage():
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
    def get_boot_time():
        return psutil.boot_time()

    @staticmethod
    def get_network_stats():
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def get_mem_usage():
        vm = psutil.virtual_memory()
        return vm.total, vm.available, vm.percent

    @staticmethod
    def get_disk_usage():
        dirs = [
            os.environ["USERPROFILE"] if sys.platform == "win32" else os.sep,
            ray.utils.get_user_temp_dir(),
        ]
        return {x: psutil.disk_usage(x) for x in dirs}

    @staticmethod
    def get_workers():
        return [
            x.as_dict(attrs=[
                "pid",
                "create_time",
                "cpu_percent",
                "cpu_times",
                "cmdline",
                "memory_info",
            ]) for x in psutil.process_iter(attrs=["cmdline"])
            if is_worker(x.info["cmdline"])
        ]

    def get_load_avg(self):
        if sys.platform == "win32":
            cpu_percent = psutil.cpu_percent()
            load = (cpu_percent, cpu_percent, cpu_percent)
        else:
            load = os.getloadavg()
        per_cpu_load = tuple((round(x / self.cpu_counts[0], 2) for x in load))
        return load, per_cpu_load

    def get_all_stats(self):
        now = to_posix_time(datetime.datetime.utcnow())
        network_stats = self.get_network_stats()

        self.network_stats_hist.append((now, network_stats))
        self.network_stats_hist = self.network_stats_hist[-7:]
        then, prev_network_stats = self.network_stats_hist[0]
        netstats = ((network_stats[0] - prev_network_stats[0]) / (now - then),
                    (network_stats[1] - prev_network_stats[1]) / (now - then))

        return {
            "now": now,
            "hostname": self.hostname,
            "ip": self.ip,
            "cpu": self.get_cpu_percent(),
            "cpus": self.cpu_counts,
            "mem": self.get_mem_usage(),
            "workers": self.get_workers(),
            "boot_time": self.get_boot_time(),
            "load_avg": self.get_load_avg(),
            "disk": self.get_disk_usage(),
            "gpus": self.get_gpu_usage(),
            "net": netstats,
        }

    def perform_iteration(self):
        """Get any changes to the log files and push updates to Redis."""
        stats = self.get_all_stats()

        self.redis_client.publish(
            self.redis_key,
            jsonify_asdict(stats),
        )

    def run(self):
        """Publish the port."""
        thread_pool = futures.ThreadPoolExecutor(max_workers=10)
        server = grpc.server(thread_pool, options=(("grpc.so_reuseport", 0), ))
        reporter_pb2_grpc.add_ReporterServiceServicer_to_server(
            ReporterServer(), server)
        port = server.add_insecure_port("[::]:0")
        server.start()
        self.redis_client.set("REPORTER_PORT:{}".format(self.ip), port)
        """Run the reporter."""
        while True:
            try:
                self.perform_iteration()
            except Exception:
                traceback.print_exc()
                pass

            time.sleep(ray_constants.REPORTER_UPDATE_INTERVAL_MS / 1000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "reporter to connect to."))
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
    args = parser.parse_args()
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    reporter = Reporter(args.redis_address, redis_password=args.redis_password)

    try:
        reporter.run()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The reporter on node {} failed with the following "
                   "error:\n{}".format(platform.node(), traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.REPORTER_DIED_ERROR, message)
        raise e
