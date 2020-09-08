from collections import defaultdict
from ray.dashboard.util import to_unix_time, format_reply_id
from base64 import b64decode
import ray
import threading
import json
import traceback
import copy
import logging
from datetime import datetime
import time
from typing import Dict
import re

from operator import itemgetter

logger = logging.getLogger(__name__)

PYCLASSNAME_RE = re.compile(r"(.+?)\(")


def _group_actors_by_python_class(actors):
    groups = defaultdict(list)
    for actor in actors.values():
        actor_title = actor.get("actorTitle")
        if not actor_title:
            groups["Unknown Class"].append(actor)
        else:
            match = PYCLASSNAME_RE.search(actor_title)
            if match:
                # Catches case of actorTitle like
                # Foo(bar, baz, [1,2,3]) -> Foo
                class_name = match.groups()[0]
                groups[class_name].append(actor)
            else:
                # Catches case of e.g. just Foo
                # in case of actor task
                groups[actor_title].append(actor)
    return groups


def _get_actor_group_stats(group):
    state_to_count = defaultdict(lambda: 0)
    executed_tasks = 0
    min_timestamp = None
    num_timestamps = 0
    sum_timestamps = 0
    now = time.time() * 1000  # convert S -> MS
    for actor in group:
        state_to_count[actor["state"]] += 1
        if "timestamp" in actor:
            if not min_timestamp or actor["timestamp"] < min_timestamp:
                min_timestamp = actor["timestamp"]
            num_timestamps += 1
            sum_timestamps += now - actor["timestamp"]
        if "numExecutedTasks" in actor:
            executed_tasks += actor["numExecutedTasks"]
    if num_timestamps > 0:
        avg_lifetime = int((sum_timestamps / num_timestamps) / 1000)
        max_lifetime = int((now - min_timestamp) / 1000)
    else:
        avg_lifetime = 0
        max_lifetime = 0
    return {
        "stateToCount": state_to_count,
        "avgLifetime": avg_lifetime,
        "maxLifetime": max_lifetime,
        "numExecutedTasks": executed_tasks,
    }


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._ip_to_hostname = {}
        self._addr_to_owner_addr = {}
        self._addr_to_actor_id = {}
        self._addr_to_extra_info_dict = {}
        self._node_stats_lock = threading.Lock()

        self._default_info = {
            "actorId": "",
            "children": {},
            "currentTaskFuncDesc": [],
            "ipAddress": "",
            "jobId": "",
            "numExecutedTasks": 0,
            "numLocalObjects": 0,
            "numObjectRefsInScope": 0,
            "port": 0,
            "state": 0,
            "taskQueueLength": 0,
            "usedObjectStoreMemory": 0,
            "usedResources": {},
        }

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.state.state._initialize_global_state(
            redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def _insert_log_counts(self):
        for ip, logs_by_pid in self._logs.items():
            hostname = self._ip_to_hostname.get(ip)
            if not hostname or hostname not in self._node_stats:
                continue
            logs_by_pid = {pid: len(logs) for pid, logs in logs_by_pid.items()}
            self._node_stats[hostname]["log_count"] = logs_by_pid

    def _insert_error_counts(self):
        for ip, errs_by_pid in self._errors.items():
            hostname = self._ip_to_hostname.get(ip)
            if not hostname or hostname not in self._node_stats:
                continue
            errs_by_pid = {pid: len(errs) for pid, errs in errs_by_pid.items()}
            self._node_stats[hostname]["error_count"] = errs_by_pid

    def _purge_outdated_stats(self):
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = to_unix_time(datetime.utcnow())
        self._node_stats = {
            k: v
            for k, v in self._node_stats.items() if current(v["now"], now)
        }

    def get_node_stats(self):
        with self._node_stats_lock:
            self._purge_outdated_stats()
            self._insert_error_counts()
            self._insert_log_counts()
            node_stats = sorted(
                (v for v in self._node_stats.values()),
                key=itemgetter("boot_time"))
            return {"clients": node_stats}

    # Gets actors in a flat way to allow for grouping by actor type.
    def get_actors(self, workers_info_by_node, infeasible_tasks, ready_tasks):
        now = time.time()
        actors: Dict[str, Dict[str, any]] = {}
        # construct flattened actor tree
        with self._node_stats_lock:
            for addr, actor_id in self._addr_to_actor_id.items():
                actors[actor_id] = copy.deepcopy(self._default_info)
                actors[actor_id].update(self._addr_to_extra_info_dict[addr])

            for node_id, workers_info in workers_info_by_node.items():
                for worker_info in workers_info:
                    if "coreWorkerStats" in worker_info:
                        core_worker_stats = worker_info["coreWorkerStats"]
                        addr = (core_worker_stats["ipAddress"],
                                str(core_worker_stats["port"]))
                        if addr in self._addr_to_actor_id:
                            actor_info = actors[self._addr_to_actor_id[addr]]
                            format_reply_id(core_worker_stats)
                            actor_info.update(core_worker_stats)
                            actor_info["averageTaskExecutionSpeed"] = round(
                                actor_info["numExecutedTasks"] /
                                (now - actor_info["timestamp"] / 1000), 2)
                            actor_info["nodeId"] = node_id
                            actor_info["pid"] = worker_info["pid"]

            def _update_from_actor_tasks(task, task_spec_type,
                                         invalid_state_type):
                actor_id = ray.utils.binary_to_hex(
                    b64decode(task[task_spec_type]["actorId"]))
                if invalid_state_type == "pendingActor":
                    task["state"] = -1
                elif invalid_state_type == "infeasibleActor":
                    task["state"] = -2
                else:
                    raise ValueError(f"Invalid argument"
                                     "invalid_state_type={invalid_state_type}")
                task["actorTitle"] = task["functionDescriptor"][
                    "pythonFunctionDescriptor"]["className"]
                format_reply_id(task)
                actors[actor_id] = task

            for infeasible_task in infeasible_tasks:
                _update_from_actor_tasks(infeasible_task,
                                         "actorCreationTaskSpec",
                                         "infeasibleActor")

            for ready_task in ready_tasks:
                _update_from_actor_tasks(ready_task, "actorCreationTaskSpec",
                                         "pendingActor")
        actor_groups = _group_actors_by_python_class(actors)
        stats_by_group = {
            name: _get_actor_group_stats(group)
            for name, group in actor_groups.items()
        }

        response_data = {}
        for name, group in actor_groups.items():
            response_data[name] = {
                "entries": group,
                "summary": stats_by_group[name]
            }
        return response_data

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

        error_channel = ray.gcs_utils.RAY_ERROR_PUBSUB_PATTERN
        p.psubscribe(error_channel)
        logger.info("NodeStats: subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.RAY_ACTOR_PUBSUB_PATTERN
        p.psubscribe(actor_channel)
        logger.info("NodeStats: subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        with self._node_stats_lock:
            for actor_data in current_actor_table.values():
                addr = (actor_data["Address"]["IPAddress"],
                        str(actor_data["Address"]["Port"]))
                owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                              str(actor_data["OwnerAddress"]["Port"]))
                self._addr_to_owner_addr[addr] = owner_addr
                self._addr_to_actor_id[addr] = actor_data["ActorID"]
                self._addr_to_extra_info_dict[addr] = {
                    "jobId": actor_data["JobID"],
                    "state": actor_data["State"],
                    "timestamp": actor_data["Timestamp"]
                }

        for x in p.listen():
            try:
                with self._node_stats_lock:
                    channel = ray.utils.decode(x["channel"])\
                                if "pattern" not in x or x["pattern"] is None\
                                else x["pattern"]
                    data = x["data"]
                    if channel == log_channel:
                        data = json.loads(ray.utils.decode(data))
                        ip = data["ip"]
                        pid = str(data["pid"])
                        self._logs[ip][pid].extend(data["lines"])
                    elif channel == str(error_channel):
                        pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(
                            data)
                        error_data = ray.gcs_utils.ErrorTableData.FromString(
                            pubsub_msg.data)
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
                    elif channel == actor_channel:
                        pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(
                            data)
                        actor_data = ray.gcs_utils.ActorTableData.FromString(
                            pubsub_msg.data)
                        addr = (actor_data.address.ip_address,
                                str(actor_data.address.port))
                        owner_addr = (actor_data.owner_address.ip_address,
                                      str(actor_data.owner_address.port))
                        self._addr_to_owner_addr[addr] = owner_addr
                        self._addr_to_actor_id[addr] = ray.utils.binary_to_hex(
                            actor_data.actor_id)
                        self._addr_to_extra_info_dict[addr] = {
                            "jobId": ray.utils.binary_to_hex(
                                actor_data.job_id),
                            "state": actor_data.state,
                            "timestamp": actor_data.timestamp
                        }
                    elif channel == ray.gcs_utils.RAY_REPORTER_PUBSUB_PATTERN:
                        data = json.loads(ray.utils.decode(data))
                        self._ip_to_hostname[data["ip"]] = data["hostname"]
                        self._node_stats[data["hostname"]] = data
                    else:
                        try:
                            data = json.loads(ray.utils.decode(data))
                        except Exception as e:
                            data = f"Failed to load data because of {e}"
                        logger.warning("Unexpected channel data received, "
                                       f"channel: {channel}, data: {data}")

            except Exception:
                logger.exception(traceback.format_exc())
                continue
