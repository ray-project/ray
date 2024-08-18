import asyncio
import logging
from typing import Any, List, Optional, Tuple

import ray.dashboard.consts as dashboard_consts
from ray.dashboard.utils import (
    Change,
    Dict,
    MutableNotificationDict,
    async_loop_forever,
    compose_state_message,
)

logger = logging.getLogger(__name__)


class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = MutableNotificationDict()
    # {job id hex(str): job table data(dict of JobTableData in gcs.proto)}
    # {node id hex(str): dashboard agent [http port(int), grpc port(int)]}
    agents = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): ip address(str)}
    node_id_to_ip = Dict()
    # {node id hex(str): hostname(str)}
    node_id_to_hostname = Dict()
    # {node id hex(str): worker list}
    node_workers = Dict()
    # {node id hex(str): {actor id hex(str): actor table data}}
    node_actors = MutableNotificationDict()
    # {worker id(str): core worker stats}
    core_worker_stats = Dict()
    # {job id hex(str): {event id(str): event dict}}
    events = Dict()

    @staticmethod
    def register_worker_update_callbacks(event_loop, thread_pool_executor):
        DataSource.node_physical_stats.signal.append(
            make_async(event_loop, thread_pool_executor, on_node_physical_stats_change)
        )
        DataSource.node_stats.signal.append(
            make_async(event_loop, thread_pool_executor, on_node_stats_change)
        )


def merge_worker_infos(
    node_stats: dict, node_physical_stats: dict
) -> Tuple[Dict[int, dict], List[dict]]:
    """
    Both inputs are for a certain node_id.
    Returns ({worker_id: workers}, [all workers]).
    The latter also contains workers that do not yet have an ID.
    """
    # Copies from node_physical_stats with 3 extra fields with default values. To be
    # updated from node_stats.
    pid_to_workers = {
        worker["pid"]: {
            **worker,
            "coreWorkerStats": {},
            "language": dashboard_consts.DEFAULT_LANGUAGE,
            "jobId": dashboard_consts.DEFAULT_JOB_ID,
        }
        for worker in node_physical_stats.get("workers", [])
    }
    for core_worker_stats in node_stats.get("coreWorkersStats", []):
        pid = core_worker_stats["pid"]
        if pid in pid_to_workers:
            worker_dict = pid_to_workers[pid]
            worker_dict["coreWorkerStats"] = core_worker_stats
            worker_dict["language"] = core_worker_stats["language"]
            worker_dict["jobId"] = core_worker_stats["jobId"]
    worker_id_to_workers = {}
    all_workers = list(pid_to_workers.values())
    for worker in all_workers:
        worker_id = worker["coreWorkerStats"].get("workerId", None)
        if worker_id is not None:
            worker_id_to_workers[worker_id] = worker
    return worker_id_to_workers, all_workers


def update_node_workers_core_worker_stats(node_id, node_stats, node_physical_stats):
    """
    Given the latest info, update the dicts. Note that some workers may not have a
    workerId. Those workers are still put to `node_workers` but not to
    `core_worker_stats`.

    Due to the nature of merging, a worker can:
    - does not exist in either
    - exists in node_physical_stats but not in node_stats
        -> updated to `node_workers` but not to `core_worker_stats`. (no worker_id)
    - exists in node_stats but not in node_physical_stats
        -> removed. (no pid, can't join)
    - exists in both
        -> updated to both.

    Update method: first upsert known workers, then remove unknown workers.
    """

    worker_id_to_workers, all_workers = merge_worker_infos(
        node_stats, node_physical_stats
    )

    for worker_id, worker_dict in worker_id_to_workers:
        DataSource.core_worker_stats[worker_id] = worker_dict

    old_workers = DataSource.node_workers.get(node_id, [])
    for old_worker in old_workers:
        worker_id = old_worker.get("workerId", None)
        if worker_id is not None and worker_id not in worker_id_to_workers:
            DataSource.core_worker_stats.pop(worker_id)

    DataSource.node_workers[node_id] = all_workers


def remove_node_workers_core_worker_stats(node_id):
    dead_workers = DataSource.node_workers.pop(node_id)
    for worker_id in dead_workers.keys():
        DataSource.core_worker_stats.pop(worker_id)


def on_node_physical_stats_change(change: Change):
    if not change.new:
        # removed node_id. Pop all.
        node_id = change.old.key
        remove_node_workers_core_worker_stats(node_id)
        return
    node_id = change.new.key
    node_stats = DataSource.node_stats.get(node_id, {})
    new_node_physical_stats = change.new.value

    update_node_workers_core_worker_stats(node_id, node_stats, new_node_physical_stats)


def on_node_stats_change(change: Change):
    if not change.new:
        # removed node_id. Pop all.
        node_id = change.old.key
        remove_node_workers_core_worker_stats(node_id)
        return
    node_id = change.new.key
    new_node_stats = change.new.value
    node_physical_stats = DataSource.node_physical_stats.get(node_id, {})

    update_node_workers_core_worker_stats(node_id, new_node_stats, node_physical_stats)


# Wrap the sync func into a ThreadPoolExecutor.
def make_async(event_loop, thread_pool_executor, f):
    async def async_f(change):
        return await event_loop.run_in_executor(thread_pool_executor, f, change)

    return async_f


class DataOrganizer:
    head_node_ip = None

    @staticmethod
    @async_loop_forever(dashboard_consts.RAY_DASHBOARD_STATS_PURGING_INTERVAL)
    async def purge():
        # Purge data that is out of date.
        # These data sources are maintained by DashboardHead,
        # we do not needs to purge them:
        #   * agents
        #   * nodes
        #   * node_id_to_ip
        #   * node_id_to_hostname
        alive_nodes = {
            node_id
            for node_id, node_info in DataSource.nodes.items()
            if node_info["state"] == "ALIVE"
        }
        for key in DataSource.node_stats.keys() - alive_nodes:
            DataSource.node_stats.pop(key)

        for key in DataSource.node_physical_stats.keys() - alive_nodes:
            DataSource.node_physical_stats.pop(key)

    @classmethod
    @async_loop_forever(dashboard_consts.RAY_DASHBOARD_STATS_UPDATING_INTERVAL)
    async def organize(cls):
        node_workers = {}
        core_worker_stats = {}
        # await inside for loop, so we create a copy of keys().
        for node_id in list(DataSource.nodes.keys()):
            workers = await cls.get_node_workers(node_id)
            for worker in workers:
                for stats in worker.get("coreWorkerStats", []):
                    worker_id = stats["workerId"]
                    core_worker_stats[worker_id] = stats
            node_workers[node_id] = workers
        DataSource.node_workers.reset(node_workers)
        DataSource.core_worker_stats.reset(core_worker_stats)

    @classmethod
    async def get_node_workers(cls, node_id):
        workers = []
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        node_stats = DataSource.node_stats.get(node_id, {})
        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        pid_to_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}
        pids_on_node = set()
        for core_worker_stats in node_stats.get("coreWorkersStats", []):
            pid = core_worker_stats["pid"]
            pids_on_node.add(pid)
            pid_to_worker_stats.setdefault(pid, []).append(core_worker_stats)
            pid_to_language[pid] = core_worker_stats["language"]
            pid_to_job_id[pid] = core_worker_stats["jobId"]

        for worker in node_physical_stats.get("workers", []):
            worker = dict(worker)
            pid = worker["pid"]
            worker["coreWorkerStats"] = pid_to_worker_stats.get(pid, [])
            worker["language"] = pid_to_language.get(
                pid, dashboard_consts.DEFAULT_LANGUAGE
            )
            worker["jobId"] = pid_to_job_id.get(pid, dashboard_consts.DEFAULT_JOB_ID)

            workers.append(worker)
        return workers

    @classmethod
    async def get_node_info(cls, node_id, get_summary=False):
        node_physical_stats = dict(DataSource.node_physical_stats.get(node_id, {}))
        node_stats = dict(DataSource.node_stats.get(node_id, {}))
        node = DataSource.nodes.get(node_id, {})

        if get_summary:
            node_physical_stats.pop("workers", None)
            node_stats.pop("workersStats", None)
        else:
            node_stats.pop("coreWorkersStats", None)
        store_stats = node_stats.get("storeStats", {})
        used = int(store_stats.get("objectStoreBytesUsed", 0))
        # objectStoreBytesAvail == total in the object_manager.cc definition.
        total = int(store_stats.get("objectStoreBytesAvail", 0))
        ray_stats = {
            "object_store_used_memory": used,
            "object_store_available_memory": total - used,
        }

        node_info = node_physical_stats
        # Merge node stats to node physical stats under raylet
        node_info["raylet"] = node_stats
        node_info["raylet"].update(ray_stats)

        node_info["status"] = node["stateSnapshot"]["state"]

        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        death_info = node.get("deathInfo", {})
        node_info["raylet"]["stateMessage"] = compose_state_message(
            death_info.get("reason", None), death_info.get("reasonMessage", None)
        )

        if not get_summary:
            # Merge actors to node physical stats
            node_info["actors"] = await DataOrganizer._get_all_actors(
                DataSource.node_actors.get(node_id, {})
            )
            # Update workers to node physical stats
            node_info["workers"] = DataSource.node_workers.get(node_id, [])

        return node_info

    @classmethod
    async def get_all_node_summary(cls):
        return [
            await DataOrganizer.get_node_info(node_id, get_summary=True)
            for node_id in DataSource.nodes.keys()
        ]

    @classmethod
    async def get_all_node_details(cls):
        return [
            await DataOrganizer.get_node_info(node_id)
            for node_id in DataSource.nodes.keys()
        ]

    @classmethod
    async def get_agent_infos(
        cls, target_node_ids: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Fetches running Agent (like HTTP/gRPC ports, IP, etc) running on every node

        :param target_node_ids: Target node ids to fetch agent info for. If omitted will
                                fetch the info for all agents
        """

        # Return all available agent infos in case no target node-ids were provided
        target_node_ids = target_node_ids or DataSource.agents.keys()

        missing_node_ids = [
            node_id for node_id in target_node_ids if node_id not in DataSource.agents
        ]
        if missing_node_ids:
            logger.warning(
                f"Agent info was not found for {missing_node_ids}"
                f" (having agent infos for {list(DataSource.agents.keys())})"
            )
            return {}

        def _create_agent_info(node_id: str):
            (http_port, grpc_port) = DataSource.agents[node_id]
            node_ip = DataSource.node_id_to_ip[node_id]

            return dict(
                ipAddress=node_ip,
                httpPort=int(http_port or -1),
                grpcPort=int(grpc_port or -1),
                httpAddress=f"{node_ip}:{http_port}",
            )

        return {node_id: _create_agent_info(node_id) for node_id in target_node_ids}

    @classmethod
    async def get_all_actors(cls):
        return await cls._get_all_actors(DataSource.actors)

    @staticmethod
    async def _get_all_actors(actors):
        result = {}
        for index, (actor_id, actor) in enumerate(actors.items()):
            result[actor_id] = await DataOrganizer._get_actor(actor)
            # There can be thousands of actors including dead ones. Processing
            # them all can take many seconds, which blocks all other requests
            # to the dashboard. The ideal solution might be to implement
            # pagination. For now, use a workaround to yield to the event loop
            # periodically, so other request handlers have a chance to run and
            # avoid long latencies.
            if index % 1000 == 0 and index > 0:
                # Canonical way to yield to the event loop:
                # https://github.com/python/asyncio/issues/284
                await asyncio.sleep(0)
        return result

    @staticmethod
    async def _get_actor(actor):
        actor = dict(actor)
        worker_id = actor["address"]["workerId"]
        core_worker_stats = DataSource.core_worker_stats.get(worker_id, {})
        actor_constructor = core_worker_stats.get(
            "actorTitle", "Unknown actor constructor"
        )
        actor["actorConstructor"] = actor_constructor
        actor.update(core_worker_stats)

        # TODO(fyrestone): remove this, give a link from actor
        # info to worker info in front-end.
        node_id = actor["address"]["rayletId"]
        pid = core_worker_stats.get("pid")
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        actor_process_stats = None
        actor_process_gpu_stats = []
        if pid:
            for process_stats in node_physical_stats.get("workers", []):
                if process_stats["pid"] == pid:
                    actor_process_stats = process_stats
                    break

            for gpu_stats in node_physical_stats.get("gpus", []):
                # gpu_stats.get("processes") can be None, an empty list or a
                # list of dictionaries.
                for process in gpu_stats.get("processesPids") or []:
                    if process["pid"] == pid:
                        actor_process_gpu_stats.append(gpu_stats)
                        break

        actor["gpus"] = actor_process_gpu_stats
        actor["processStats"] = actor_process_stats
        actor["mem"] = node_physical_stats.get("mem", [])
        return actor
