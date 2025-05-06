import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pyspark.util import inheritable_thread_target

from ray.util.spark.cluster_init import _start_ray_worker_nodes


class SparkJobServerRequestHandler(BaseHTTPRequestHandler):
    def setup(self) -> None:
        super().setup()
        self._handler_lock = threading.RLock()
        self._created_node_id_set = set()
        self._logger = logging.getLogger(__name__)
        if "RAY_ON_SPARK_JOB_SERVER_VERBOSE" in os.environ:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.WARN)

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def handle_POST(self, path, data):
        path_parts = Path(path).parts[1:]

        spark_job_group_id = data["spark_job_group_id"]

        if path_parts[0] == "create_node":
            assert len(path_parts) == 1, f"Illegal request path: {path}"
            spark_job_group_desc = data["spark_job_group_desc"]
            using_stage_scheduling = data["using_stage_scheduling"]
            ray_head_ip = data["ray_head_ip"]
            ray_head_port = data["ray_head_port"]
            ray_temp_dir = data["ray_temp_dir"]
            num_cpus_per_node = data["num_cpus_per_node"]
            num_gpus_per_node = data["num_gpus_per_node"]
            heap_memory_per_node = data["heap_memory_per_node"]
            object_store_memory_per_node = data["object_store_memory_per_node"]
            worker_node_options = data["worker_node_options"]
            collect_log_to_path = data["collect_log_to_path"]
            node_id = data["node_id"]
            self._created_node_id_set.add(node_id)

            def start_ray_worker_thread_fn():
                try:
                    err_msg = _start_ray_worker_nodes(
                        spark_job_server=self.server,
                        spark_job_group_id=spark_job_group_id,
                        spark_job_group_desc=spark_job_group_desc,
                        num_worker_nodes=1,
                        using_stage_scheduling=using_stage_scheduling,
                        ray_head_ip=ray_head_ip,
                        ray_head_port=ray_head_port,
                        ray_temp_dir=ray_temp_dir,
                        num_cpus_per_node=num_cpus_per_node,
                        num_gpus_per_node=num_gpus_per_node,
                        heap_memory_per_node=heap_memory_per_node,
                        object_store_memory_per_node=object_store_memory_per_node,
                        worker_node_options=worker_node_options,
                        collect_log_to_path=collect_log_to_path,
                        node_id=node_id,
                    )
                    if err_msg:
                        self._logger.warning(
                            f"Spark job {spark_job_group_id} hosting Ray worker node "
                            f"launching failed, error:\n{err_msg}"
                        )
                except Exception:
                    if spark_job_group_id in self.server.task_status_dict:
                        self.server.task_status_dict.pop(spark_job_group_id)

                    msg = (
                        f"Spark job {spark_job_group_id} hosting Ray worker node exit."
                    )
                    if self._logger.level > logging.DEBUG:
                        self._logger.warning(
                            f"{msg} To see details, you can set "
                            "'RAY_ON_SPARK_JOB_SERVER_VERBOSE' environmental variable "
                            "to '1' before calling 'ray.util.spark.setup_ray_cluster'."
                        )
                    else:
                        # This branch is only for debugging Ray-on-Spark purpose.
                        # User can configure 'RAY_ON_SPARK_JOB_SERVER_VERBOSE'
                        # environment variable to make the spark job server logging
                        # showing full exception stack here.
                        self._logger.debug(msg, exc_info=True)

            threading.Thread(
                target=inheritable_thread_target(start_ray_worker_thread_fn),
                args=(),
                daemon=True,
            ).start()

            self.server.task_status_dict[spark_job_group_id] = "pending"
            return {}

        elif path_parts[0] == "check_node_id_availability":
            node_id = data["node_id"]
            with self._handler_lock:
                if node_id in self._created_node_id_set:
                    # If the node with the node id has been created,
                    # it shouldn't be created twice so fail fast here.
                    # The case happens when a Ray node is down unexpected
                    # caused by spark worker node down and spark tries to
                    # reschedule the spark task, so it triggers node
                    # creation with duplicated node id.
                    return {"available": False}
                else:
                    self._created_node_id_set.add(node_id)
                    return {"available": True}

        elif path_parts[0] == "terminate_node":
            assert len(path_parts) == 1, f"Illegal request path: {path}"
            self.server.spark.sparkContext.cancelJobGroup(spark_job_group_id)
            if spark_job_group_id in self.server.task_status_dict:
                self.server.task_status_dict.pop(spark_job_group_id)
            return {}

        elif path_parts[0] == "notify_task_launched":
            if spark_job_group_id in self.server.task_status_dict:
                # Note that if `spark_job_group_id` not in task_status_dict,
                # the task has been terminated
                self.server.task_status_dict[spark_job_group_id] = "running"
                self._logger.info(f"Spark task in {spark_job_group_id} has started.")
            return {}

        elif path_parts[0] == "query_task_status":
            if spark_job_group_id in self.server.task_status_dict:
                return {"status": self.server.task_status_dict[spark_job_group_id]}
            else:
                return {"status": "terminated"}

        elif path_parts[0] == "query_last_worker_err":
            return {"last_worker_err": self.server.last_worker_error}

        else:
            raise ValueError(f"Illegal request path: {path}")

    def do_POST(self):
        """Reads post request body"""
        self._set_headers()
        content_len = int(self.headers["content-length"])
        content_type = self.headers["content-type"]
        assert content_type == "application/json"
        path = self.path
        post_body = self.rfile.read(content_len).decode("utf-8")
        post_body_json = json.loads(post_body)
        with self._handler_lock:
            response_body_json = self.handle_POST(path, post_body_json)
        response_body = json.dumps(response_body_json)
        self.wfile.write(response_body.encode("utf-8"))

    def log_request(self, code="-", size="-"):
        # Make logs less verbose.
        pass


class SparkJobServer(ThreadingHTTPServer):
    """
    High level design:

    1. In Ray on spark autoscaling mode, How to start and terminate Ray worker node ?

    It uses spark job to launch Ray worker node,
    and each spark job contains only one spark task, the corresponding spark task
    creates Ray worker node as subprocess.
    When autoscaler request terminating specific Ray worker node, it cancels
    corresponding spark job to trigger Ray worker node termination.
    Because we can only cancel spark job not spark task when we need to scale
    down a Ray worker node. So we have to have one spark job for each Ray worker node.

    2. How to create / cancel spark job from spark node provider?

    Spark node provider runs in autoscaler process that is different process
    than the one that executes "setup_ray_cluster" API. User calls "setup_ray_cluster"
    API in spark application driver node, and the semantic is "setup_ray_cluster"
    requests spark resources from this spark application.
    Internally, "setup_ray_cluster" should use "spark session" instance to request
    spark application resources. But spark node provider runs in another python
    process, in order to share spark session to the separate NodeProvider process,
    it sets up a spark job server that runs inside spark application driver process
    (the process that calls "setup_ray_cluster" API), and in NodeProvider process,
    it sends RPC request to the spark job server for creating spark jobs in the
    spark application.
    Note that we cannot create another spark session in NodeProvider process,
    because if doing so, it means we create another spark application, and then
    it causes NodeProvider requests resources belonging to the new spark application,
    but we need to ensure all requested spark resources belong to
    the original spark application that calls "setup_ray_cluster" API.

    Note:
    The server must inherit ThreadingHTTPServer because request handler uses
    the active spark session in current process to create spark jobs, so all request
    handler must be running in current process.
    """

    def __init__(self, server_address, spark, ray_node_custom_env):
        super().__init__(server_address, SparkJobServerRequestHandler)
        self.spark = spark

        # For ray on spark autoscaling mode,
        # for each ray worker node, we create an individual spark job
        # to launch it, the corresponding spark job has only one
        # spark task that starts ray worker node, and the spark job
        # is assigned with a unique spark job group ID that is used
        # to cancel this spark job (i.e., kill corresponding ray worker node).
        # Each spark task has status of pending, running, or terminated.
        # the task_status_dict key is spark job group id,
        # and value is the corresponding spark task status.
        # each spark task holds a ray worker node.
        self.task_status_dict = {}
        self.last_worker_error = None
        self.ray_node_custom_env = ray_node_custom_env

    def shutdown(self) -> None:
        super().shutdown()
        for spark_job_group_id in self.task_status_dict:
            self.spark.sparkContext.cancelJobGroup(spark_job_group_id)
        # Sleep 1 second to wait for all spark job cancellation
        # The spark job cancellation will do things asyncly in a background thread,
        # On Databricks platform, when detaching a notebook, it triggers SIGTERM
        # and then sigterm handler triggers Ray cluster shutdown, without sleep,
        # after the SIGTERM handler execution the process is killed and then
        # these cancelling spark job background threads are killed.
        time.sleep(1)


def _start_spark_job_server(host, port, spark, ray_node_custom_env):
    server = SparkJobServer((host, port), spark, ray_node_custom_env)

    def run_server():
        server.serve_forever()

    server_thread = threading.Thread(target=run_server)
    server_thread.setDaemon(True)
    server_thread.start()

    return server
