import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading

from pyspark.util import inheritable_thread_target

from ray.util.spark.cluster_init import get_spark_session
from ray.util.spark.cluster_init import _start_ray_worker_nodes
import logging


_logger = logging.getLogger("ray.autoscaler._private.spark.spark_job_server")
_logger.setLevel(logging.WARN)


class SparkJobServerRequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
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

            def start_ray_worker_thread_fn():
                try:
                    _start_ray_worker_nodes(
                        spark=self.server.spark,
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
                        autoscale_mode=True,
                        spark_job_server_port=self.server.server_address[1],
                    )
                except Exception:
                    if spark_job_group_id in self.server.task_status_dict:
                        self.server.task_status_dict.pop(spark_job_group_id)

                    # TODO: Refine error handling.
                    _logger.warning(
                        f"Spark job {spark_job_group_id} hosting Ray worker node exit."
                    )

            # TODO: register databricks background spark job group.
            threading.Thread(
                target=inheritable_thread_target(start_ray_worker_thread_fn),
                args=(),
                daemon=True,
            ).start()

            self.server.task_status_dict[spark_job_group_id] = "pending"
            return {}

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
                _logger.info(f"Spark task in {spark_job_group_id} has started.")
            return {}

        elif path_parts[0] == "query_task_status":
            if spark_job_group_id in self.server.task_status_dict:
                return {"status": self.server.task_status_dict[spark_job_group_id]}
            else:
                return {"status": "terminated"}

        else:
            raise ValueError(f"Illegal request path: {path}")

    def do_POST(self):
        '''Reads post request body'''
        """
        curl -X POST http://localhost/ -H 'Content-Type: application/json' -d '{"login":"my_login","password":"my_password"}'
        """
        self._set_headers()
        content_len = int(self.headers['content-length'])
        content_type = self.headers['content-type']
        assert content_type == "application/json"
        path = self.path
        post_body = self.rfile.read(content_len).decode('utf-8')
        post_body_json = json.loads(post_body)
        response_body_json = self.handle_POST(path, post_body_json)
        response_body = json.dumps(response_body_json)
        self.wfile.write(response_body.encode("utf-8"))

    def log_request(self, code='-', size='-'):
        # Make logs less verbose.
        pass


class SparkJobServer(ThreadingHTTPServer):
    """
    The http server that is used to launch spark jobs for holding ray worker nodes
    Note: The server must inherit ThreadingHTTPServer because request handler uses
    the active spark session in current process to create spark jobs, so all request
    handler must be running in current process.
    """

    spark = None

    def __init__(self, server_address, spark):
        super().__init__(server_address, SparkJobServerRequestHandler)
        self.spark = spark
        self.task_status_dict = {}

    def shutdown(self) -> None:
        super().shutdown()
        for spark_job_group_id in self.task_status_dict:
            self.server.spark.sparkContext.cancelJobGroup(spark_job_group_id)


def _start_spark_job_server(host, port, spark):
    server = SparkJobServer((host, port), spark)

    def run_server():
        server.serve_forever()

    server_thread = threading.Thread(target=run_server)
    server_thread.setDaemon(True)
    server_thread.start()

    return server
