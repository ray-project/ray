from abc import ABC, abstractmethod
from pyarrow import fs
import os
import json
import threading
import logging
import time
from datetime import datetime
import subprocess
import ray
import ray.dashboard.optional_utils as optional_utils
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.dashboard.modules.job.utils import encrypt_aes
from typing import List, Any

logger = logging.getLogger(__name__)

DEFAULT_EVENT_LOG_FILENAME = "ray_event_log"
DEFAULT_HISTORY_SERVER_EVENT_LOG = os.getenv("BYTED_RAY_HISTORY_SERVER_EVENT_LOG", None)


class Storage(ABC):
    @abstractmethod
    def append(self, data: List[str]):
        pass

    @abstractmethod
    def read(self, path):
        pass


class LocalFileStorage(Storage):
    def __init__(self, path: str, dir: str):
        logger.info(f"Init LocalFileStorage path: {path}")
        print(f"Init LocalFileStorage path: {path} dir: {dir}")
        self.base_path = path
        self.dir = dir
        self.file_path = None
        self.path = path
        self.lock = threading.Lock()

    def append(self, data: List[str]):
        if not self.path:
            return

        with self.lock:
            try:
                if not self.file_path:
                    self.file_path = (
                        f"{self.base_path}/{self.dir}/{DEFAULT_EVENT_LOG_FILENAME}"
                    )
                    print(f"file_path {self.file_path}")
                    if os.path.exists(self.file_path):
                        os.remove(self.file_path)
                    os.makedirs(f"{self.base_path}/{self.dir}", exist_ok=True)

                with open(self.file_path, "a") as f:
                    for d in data:
                        f.write(f"{d}\n")
            except Exception as e:
                logger.error(
                    f"Exception occur when append to {self.file_path}, error, {e}"
                )
                return

    def read(self, path: str):
        logger.info("LocalFileStorage read")
        if not self.path:
            logger.info("LocalFileStorage read 1")
            return None

        with self.lock:
            file_path = f"{self.base_path}/{self.dir}/{path}"
            try:
                logger.info(f"LocalFileStorage read {file_path}")
                with open(file_path, "r") as f:
                    return f.read()
            except Exception as e:
                logger.error(f"read {file_path} exception: {e}")
                return None

    def get_update_timestamp(self, file: str):
        return 0


class HdfsStorage(Storage):
    def __init__(self, uri, dir):
        self.uri = uri
        self.hdfs_fs = None
        self.hdfs_path = None
        self.dir = dir
        self.file_path = None
        self.lock = threading.Lock()
        self.failed_count = 0

    def append(self, data: List[str]):
        if self.failed_count >= 5:
            logger.error(
                f"skip hdfs uploading to {self.file_path}, maybe you have no permissions to write this path"
            )
            return
        with self.lock:
            try:
                # NOTE(wangwanxing), An unknown error is reported when initializing a file system into __init__
                if not self._is_filesystem_initialized():
                    self.init_filesystem()

                if not self.file_path:
                    self.file_path = (
                        f"{self.hdfs_path}/{self.dir}/{DEFAULT_EVENT_LOG_FILENAME}"
                    )
                    with self.hdfs_fs.open_output_stream(self.file_path):
                        pass

                with self.hdfs_fs.open_append_stream(self.file_path) as dest:
                    for d in data:
                        dest.write(f"{d}\n".encode())
                self.failed_count = 0
            except Exception:
                self.failed_count += 1
                logger.error(f"Exception occur when append to {self.file_path}")
                return

    def read(self, path: str):
        """
        path: The relative path to the file. For example, hdfs://haruna/home/byte_inf_compute/user/wangwanxing/my_test_cluster_name3/ray_event_log
              path should be "my_test_cluster_name3/ray_event_log"
        """
        with self.lock:
            if not self._is_filesystem_initialized():
                self.init_filesystem()

            file_path = f"{self.hdfs_path}/{path}"
            try:
                with self.hdfs_fs.open_input_stream(file_path) as f:
                    content = f.readall().decode()
                    # print(f"content type: {type(content)} size: {len(content)}")
                    return content
            except Exception as e:
                logger.error(f"Exception occur when read from hdfs: {e}")
                return None

    def _is_filesystem_initialized(self):
        return self.hdfs_fs is not None and self.hdfs_path is not None

    def init_filesystem(self):
        logger.info("init_filesystem")
        self.hdfs_fs, self.hdfs_path = fs.FileSystem.from_uri(self.uri)

    def close(self):
        self.fs.close()

    def get_update_timestamp(self, file: str) -> int:
        # TODO use pyarrow hdfs
        command = "hadoop fs -ls " + file + " | awk -F' ' '{print $6\" \"$7}' "
        output = subprocess.check_output(command, shell=True)
        file_date = datetime.strptime(output.decode("utf-8").strip(), "%Y-%m-%d %H:%M")
        return int(file_date.timestamp())


@ray.remote
class HDFSActor:
    def __init__(self, uri, dir):
        self.storage = HdfsStorage(uri, dir)
        pass

    def append(self, data: List[str]):
        self.storage.append(data)

    def read(self, path: str):
        return self.storage.read(path)


class ActorHdfsStorage(Storage):
    def __init__(self, uri, dir, address):
        self.uri = uri
        self.dir = dir
        self.actor = None
        self.gcs_address = address

    def append(self, data: List[str]):
        if not self.actor:
            self._init_actor_if_needed()
        self.actor.append.remote(data)

    def read(self, path: str):
        if not self.actor:
            self._init_actor_if_needed()
        return ray.get(self.actor.read.remote(path))

    def _init_actor_if_needed(self):
        if not ray.is_initialized():
            try:
                logger.info(f"Connecting to ray with address={self.gcs_address}")
                # Init ray without logging to driver
                # to avoid infinite logging issue.
                ray.init(
                    address=self.gcs_address,
                    log_to_driver=False,
                    configure_logging=False,
                    namespace=optional_utils.RAY_INTERNAL_DASHBOARD_NAMESPACE,
                )
            except Exception as e:
                ray.shutdown()
                raise e from None

        if not self.actor:
            self.actor = HDFSActor.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=ray.get_runtime_context().node_id, soft=False
                ),
                max_restarts=-1,
                num_cpus=0,
            ).remote(self.uri, self.dir)


class TOSStorage(Storage):
    def __init__(self, uri, key, dir, timeout=10):
        """
        uri: TOS endpoint and bucket, for example: tos://<endpoint>/<bucket>/prefix.
        dir: TOS path to read and write.
        """
        fields = uri[len("tos://") :].split("/")
        assert len(fields) >= 3
        self.endpoint, self.bucket = fields[0], fields[1]
        fields.append(dir)
        self.prefix = "/".join(fields[2:])

        import bytedtos

        self.client = bytedtos.Client(
            self.bucket,
            access_key=key,
            endpoint=self.endpoint,
            timeout=timeout,
            connect_timeout=timeout,
        )
        self.data = ""
        self.failed_count = 0

    def append(self, data: List[str], path=DEFAULT_EVENT_LOG_FILENAME):
        if self.failed_count >= 5:
            logger.error(
                f"skip TOS uploading to {self.bucket}, maybe you have no permissions to write this path"
            )
            return

        try:
            # NOTE(wuxibin): TOS doesn't support appending to file
            self.data += "\n".join(data) + "\n"
            self.client.put_object(f"{self.prefix}/{path}", self.data)
        except Exception as e:
            self.failed_count += 1
            logger.error(f"Exception occur when append to TOS: {e}")

    def write(self, data: bytes, path: str):
        self.client.put_object(f"{self.prefix}/{path}", data)

    def read(self, path: str, decode=True):
        try:
            resp = self.client.get_object(f"{self.prefix}/{path}")
            return resp.data.decode() if decode else resp.data
        except Exception as e:
            logger.error(f"Exception occur when read from TOS: {e}")
            return None

    def get_update_timestamp(self, file: str) -> int:
        try:
            info = self.client.head_object(f"{self.prefix}/{file}")
            return info.last_modify_time
        except Exception as e:
            logger.error(f"Exception occur when read from TOS: {e}")
            return 0


class EventLogCache:
    def __init__(self):
        # cluster_name -> event log json string
        self.cache = {}
        # cluster_name -> the last query cache timestamp
        self.cache_query_timestamp = {}
        # cluster_name -> the last hdfs file modified time, default is zero
        self.cache_update_timestamp = {}
        self.lock = threading.Lock()
        # determined the query is timeout by cache_query_timestamp
        self.default_timeout_s = 60 * 5

    def add(self, key: str, value: str, file_timestamp: int = 0):
        with self.lock:
            # if key in self.cache.keys():
            #     logger.warning(f"Ignore add duplicate key {key} to EventLogCache.")
            #     return
            self.cache[key] = value
            # zero means no hdfs mode
            if file_timestamp > 0:
                self.cache_update_timestamp[key] = file_timestamp

    def get(self, key: str):
        with self.lock:
            is_timeout = False
            now_time = int(time.time())
            if key in self.cache_query_timestamp:
                if self.cache_query_timestamp[key] + self.default_timeout_s < now_time:
                    is_timeout = True
                    self.cache_query_timestamp[key] = now_time
            else:
                self.cache_query_timestamp[key] = now_time
            if key not in self.cache_update_timestamp:
                self.cache_update_timestamp[key] = 0
            return self.cache.get(key), is_timeout, self.cache_update_timestamp.get(key)


def create_history_server_storage(ray_cluster_name=None, gcs_address=None):
    history_server_storage = None
    history_server_event_log = DEFAULT_HISTORY_SERVER_EVENT_LOG
    if ray_cluster_name is None:
        ray_cluster_name = os.getenv("BYTED_RAY_CLUSTER", None)
    logger.info(
        f"history server event log: {history_server_event_log}, ray cluster name: {ray_cluster_name}"
    )
    if (
        history_server_event_log
        and history_server_event_log.startswith("hdfs://")
        and ray_cluster_name
    ):
        history_server_storage = ActorHdfsStorage(
            history_server_event_log, ray_cluster_name, gcs_address
        )
    elif (
        history_server_event_log
        and history_server_event_log.startswith("tos://")
        and ray_cluster_name
    ):
        access_key = os.getenv("BYTED_RAY_TOS_ACCESS_KEY", None)
        history_server_storage = TOSStorage(
            history_server_event_log, access_key, ray_cluster_name
        )
    elif history_server_event_log and history_server_event_log.startswith("/"):
        history_server_storage = LocalFileStorage(
            history_server_event_log, ray_cluster_name
        )
    else:
        history_server_storage = LocalFileStorage(None, None)

    return history_server_storage


# {
# "event_type": "JOB",
# "job":
#   {
#     "submission_id": "ray_submit_xxxx",
#     "job_info": {
#       "entrypoint": "python test.py",
#       "status": "SUCCEED"
#     }
#   }
# }
def append_job_event(storage: Storage, submission_id: str, job_info_json: str):
    json_obj = {
        "event_type": "JOB",
        "job": {"submission_id": submission_id, "job_info": job_info_json},
    }

    json_str = json.dumps(json_obj)
    storage.append([json_str])


# {
# "event_type": "ACTOR",
# "actor":
#   {
#     "actor_id": "bce926b88c54ebb4e8b790bd01000000",
#     "actor_info": {
# "actorId": "bce926b88c54ebb4e8b790bd01000000",
# "jobId": "01000000",
# "address": {
#     "rayletId": "56b97436e67393f15565e024278867700c2bd0ec6ca9e0dac7261fd1",
#     "ipAddress": "127.0.0.1",
#     "port": 55428,
#     "workerId": "5a1c3eceb99da5dc6a7d8769b8fec657b73be8bdbc8b97bf3fdaee9f",
# },
# "name": "SERVE_REPLICA::MyModelDeployment#wxgUmq_0",
# "className": "ServeReplica:MyModelDeployment",
# "state": "DEAD",
# "numRestarts": "0",
# "timestamp": 1689060097299.0,
# "pid": 29686,
# "startTime": 1689060085642,
# "endTime": 1689060097298,
# "actorClass": "ServeReplica:MyModelDeployment",
# "exitDetail": "The actor is dead because its node has died. Node Id: 56b97436e67393f15565e024278867700c2bd0ec6ca9e0dac7261fd1",
# "requiredResources": {"CPU": 1.0},
# }
#   }
# }
def append_actor_events(storage: Storage, actor_infos: List[Any]):
    def to_json_str(actor_info):
        json_obj = {
            "event_type": "ACTOR",
            "actor": {"actor_id": actor_info["actorId"], "actor_info": actor_info},
        }

        return json.dumps(json_obj)

    json_str_list = list(map(to_json_str, actor_infos))
    storage.append(json_str_list)


def append_node_event(storage: Storage, node_id: str, node_info: str):
    json_obj = {
        "event_type": "NODE",
        "node": {"node_id": node_id, "node_info": node_info},
    }

    json_str = json.dumps(json_obj)
    storage.append([json_str])


def read_event_log(storage: Storage, cluster_name: str):
    return storage.read(DEFAULT_EVENT_LOG_FILENAME)


def get_event_log(cache: EventLogCache, cluster_name):
    # get event log in cache
    try:
        event_log_str, is_timeout, last_timestamp = cache.get(cluster_name)
        if event_log_str:
            if is_timeout:
                storage = create_history_server_storage(cluster_name)
                update_timestamp = storage.get_update_timestamp(
                    DEFAULT_EVENT_LOG_FILENAME
                )
                if last_timestamp < update_timestamp:
                    logger.info(f"{cluster_name} log is out of date")
                    event_log_str = read_event_log(storage, cluster_name)
                    cache.add(cluster_name, event_log_str, update_timestamp)
                    return event_log_str
            logger.info(f"{cluster_name} find event log in cache.")
            return event_log_str

        logger.info(f"{cluster_name} try to get event log in hdfs.")
        storage = create_history_server_storage(cluster_name)
        update_timestamp = storage.get_update_timestamp(DEFAULT_EVENT_LOG_FILENAME)
        event_log_str = read_event_log(storage, cluster_name)
        cache.add(cluster_name, event_log_str, update_timestamp)
        return event_log_str
    except Exception as e:
        logger.info(f"get exception when querying {cluster_name}, error: {e}")
        return None


def get_cluster_all_events(event_cache, cluster_name, event_type):
    event_log_str = get_event_log(event_cache, cluster_name)

    if not event_log_str:
        return {}

    event_log_lines = event_log_str.rstrip().split("\n")
    events = {}
    for line in event_log_lines:
        event = json.loads(line)
        if event["event_type"].lower() != event_type:
            continue

        event_id = event[event_type][event_type + "_id"]
        events[event_id] = event[event_type][event_type + "_info"]

    return events


GODEL_LOGAGENT_URL_BASE = os.environ.get("BYTED_RAY_LOGAGENT_LINK", None)
GODEL_LOGAGENT_KEY = os.environ.get("BYTED_RAY_LOGAGENT_KEY", None)


def generate_logagent_url(
    psm: str, hostip: str, podname: str, containername: str, logname: str = None
):
    if GODEL_LOGAGENT_KEY is None or GODEL_LOGAGENT_URL_BASE is None:
        return None

    if hostip.startswith("[") and hostip.endswith("]"):
        hostip = hostip[1:-1]
    params = (
        f"psm={psm}&hostip={hostip}&podname={podname}&containername={containername}"
    )
    if logname is not None:
        params = f"{params}&logname={logname}"
    params = f"{params}&username=xxx"

    code = encrypt_aes(GODEL_LOGAGENT_KEY, params)
    if code is None:
        return None
    return f"{GODEL_LOGAGENT_URL_BASE}code={code.decode('utf-8')}"
