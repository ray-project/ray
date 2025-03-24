import threading
import logging
import socket
import time
import redis
import ray._private.services
import ray._private.gcs_utils
from ray.ha import HeadNodeLeaderSelector
import ray._private.ray_constants as ray_constants
import os
import requests
import json

logger = logging.getLogger(__name__)


def is_service_available(ip, port):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        result = sock.connect_ex((ip, int(port)))
        sock.close()
        return 0 == result
    except Exception:
        if sock is not None:
            sock.close()
        return False


def waiting_for_server_stopped(address, max_time):
    if address is None or len(address) <= 0:
        return True
    start_time = time.time()
    use_time = 0
    ip, port = address.split(":")
    while use_time <= max_time:
        if not is_service_available(ip, port):
            return True, time.time() - start_time
        time.sleep(0.5)
        use_time = time.time() - start_time
    return False, use_time


def parse_head_num(data):
    head_num = 0
    groups = data.get("data", {}).get("head", [])
    for group in groups:
        for group_name in group.keys():
            head_num = head_num + group.get(group_name, {}).get("replicasTotal", 0)

    return head_num


def get_cluster_head_num(timeout):
    cluster_name = os.environ.get("CLUSTER_NAME", "")
    ray_operator_address = os.environ.get("RAY_OPERATOR_ADDRESS", "")
    k8s_namespace = os.environ.get("NAMESPACE", "")
    head_group_name = os.environ.get("RAY_SHAPE_GROUP", "default")
    url = f"http://{ray_operator_address}/elasticity/v2/workerNodes/status"
    data = {
        "name": cluster_name,
        "namespace": k8s_namespace,
        "containsPodInfo": False,
        "headNode": {"group": head_group_name},
    }
    try:
        response = requests.post(url, data=json.dumps(data), timeout=timeout)
        if response.status_code >= 200 and response.status_code < 300:
            resp_json = response.json()
            if resp_json.get("success", False):
                return parse_head_num(resp_json)
            else:
                raise ConnectionError(
                    "The response of request has failed, resp:{resp_json}."
                )
        else:
            raise ConnectionError(
                f"The status code of request is error, "
                f"status code:{response.status_code}, resp:{response}."
            )
    except Exception as err:
        logger.info(
            f"Failed to get cluster head num, "
            f"url:{url}, data:{data}, exception:{str(err)}"
        )
        return -1


class RedisBasedLeaderSelector(HeadNodeLeaderSelector):
    _real_failure_count = 0
    _timer = None
    _leader_name = None

    def __init__(self, ray_params, redis_address, node_ip_address):
        super().__init__()
        self._redis_address = redis_address
        self._redis_username = ray_params.redis_username
        self._redis_password = ray_params.redis_password
        self._node_ip_address = node_ip_address
        gcs_server_port = (
            0 if ray_params.gcs_server_port is None else ray_params.gcs_server_port
        )
        self._init_gcs_address = node_ip_address + ":" + str(gcs_server_port)
        suffix = str(int(time.time()))
        self.name = (self._init_gcs_address + "-" + suffix).encode("UTF-8")
        logger.info("Initialized redis leader selector with %s.", self._config)

    def start(self):
        self._is_running = True
        redis_ip_address, redis_port = self._redis_address.split(":")
        ray._private.services.wait_for_redis_to_start(
            redis_ip_address,
            redis_port,
            username=self._redis_username,
            password=self._redis_password,
        )
        self._redis_client = redis.Redis(
            host=redis_ip_address,
            port=int(redis_port),
            username=self._redis_username,
            password=self._redis_password,
            socket_timeout=self._config.connect_timeout_s,
            socket_connect_timeout=self._config.connect_timeout_s,
        )
        logger.info("Success to start redis leader selecotr. name:%s", self.name)
        self.do_election()

    def stop(self):
        self.set_role_type(ray_constants.HEAD_ROLE_STANDBY)
        self._is_running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._redis_client is not None:
            self._redis_client.close()
            self._redis_client = None

    def do_election(self):
        if self._is_running:
            self.check_leader()
            if self._is_running:
                self._timer = threading.Timer(
                    self._config.check_interval_s, self.do_election
                )
                self._timer.start()
        else:
            logger.warning("Timer still running while leader selector stopped.")

    def check_leader(self):
        try:
            isSetSuccess = self._redis_client.set(
                ray_constants.HEAD_NODE_LEADER_ELECTION_KEY,
                self.name,
                px=self._config.key_expire_time_ms,
                nx=True,
            )
            if isSetSuccess:
                self.set_role_type(ray_constants.HEAD_ROLE_ACTIVE)
                self._real_failure_count = 0
                logger.info(
                    "This head node preempted lock, "
                    "become the active head node, name:%s.",
                    self.name,
                )
            else:
                self.expire_leader_key()
        except Exception:
            logger.error("%s node failed to check leader.", self.name, exc_info=True)
            self._real_failure_count += 1
            if self._real_failure_count > self._config.max_failure_count:
                head_num = get_cluster_head_num(self._config.connect_timeout_s)
                if self.is_leader() and head_num == 1:
                    if self._real_failure_count % self._config.max_failure_count == 1:
                        logger.warning(
                            "Because current node is leader and "
                            "cluster head number is only one,"
                            "will not change role to standby."
                        )
                else:
                    logger.error(
                        "This leader selector will stop for "
                        "exception %d times more then %d. "
                        "The role will change from %s to standby, "
                        "head num: %d.",
                        self._real_failure_count,
                        self._config.max_failure_count,
                        self.get_role_type(),
                        head_num,
                    )
                    self.stop()

    def expire_leader_key(self):
        leader_name = self._redis_client.get(
            ray_constants.HEAD_NODE_LEADER_ELECTION_KEY
        )
        if leader_name == self.name:
            isSuccess = self._redis_client.pexpire(
                ray_constants.HEAD_NODE_LEADER_ELECTION_KEY,
                self._config.key_expire_time_ms,
            )
            if not isSuccess:
                self._real_failure_count += 1
                logger.error(
                    "This active head node expire leader "
                    "key failed, failure count:%d.",
                    self._real_failure_count,
                )
                if self._real_failure_count > self._config.max_failure_count:
                    logger.error(
                        "This active head node role "
                        "downgraded to standby for expire failed"
                        " %d times more then %d.",
                        self._real_failure_count,
                        self._config.max_failure_count,
                    )
                    self.stop()
            self._real_failure_count = 0
            self.set_role_type(ray_constants.HEAD_ROLE_ACTIVE)
        else:
            if self.is_leader():
                logger.error(
                    "This active head node role downgraded "
                    "to standby for leader key changed, active:%s.",
                    str(leader_name),
                )
                self.stop()
            else:
                if self._leader_name != str(leader_name):
                    self._leader_name = str(leader_name)
                    logger.info(
                        "This head node role is standby now. " "active node is %s.",
                        str(leader_name),
                    )
                else:
                    logger.debug(
                        "This head node role is standby now. " "active node is %s.",
                        str(leader_name),
                    )
            self.set_role_type(ray_constants.HEAD_ROLE_STANDBY)

    def do_action_after_be_active(self):
        expired_gcs_address = self._redis_client.get(ray_constants.GCS_ADDRESS_KEY)
        if expired_gcs_address:
            pre_gcs_address = str(expired_gcs_address, encoding="utf-8")
            self._redis_client.set(
                ray_constants.GCS_ADDRESS_KEY, self._init_gcs_address
            )
            logger.info(
                "Reset gcs address from %s to invalid gcs address:%s",
                pre_gcs_address,
                self._init_gcs_address,
            )
            time.sleep(self._config.wait_time_after_be_active_s)
            max_wait_time = self._config.wait_pre_gcs_stop_max_time_s
            is_disconnect, use_time = waiting_for_server_stopped(
                pre_gcs_address, max_wait_time
            )
            if is_disconnect:
                logger.info(
                    "After waiting for %f s, the previous gcs(%s) has "
                    "stopped working, going to start startup process.",
                    use_time,
                    pre_gcs_address,
                )
                return True
            else:
                logger.error(
                    "After waiting for %d s, the previous gcs(%s) still"
                    " working. this maybe casue some wrong.",
                    max_wait_time,
                    pre_gcs_address,
                )
                return False
        else:
            self._redis_client.set(
                ray_constants.GCS_ADDRESS_KEY, self._init_gcs_address
            )
            logger.info(
                "Reset gcs address to %s",
                self._init_gcs_address,
            )
        return True

    def node_wait_to_be_active(self):
        logger.info("This head node is waiting to to be active...")
        while True:
            if not self.is_running():
                logger.error(
                    "The leader selector has stopped in waiting," " will restart it."
                )
                self.start()
                continue

            if self.is_leader():
                logger.info(
                    "This head node changed from standby to active, "
                    "start the startup process..."
                )
                self.do_action_after_be_active()
                break
            logger.debug(
                "This head node role is %s, waiting to be acitve node.",
                self.get_role_type(),
            )
            time.sleep(0.2)
