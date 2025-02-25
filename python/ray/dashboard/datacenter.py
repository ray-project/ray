import logging
from typing import List, Optional

import ray.dashboard.consts as dashboard_consts
from ray._private.utils import (
    get_or_create_event_loop,
    parse_pg_formatted_resources_to_original,
)
from ray.dashboard.utils import (
    Dict,
    MutableNotificationDict,
    async_loop_forever,
    compose_state_message,
)

logger = logging.getLogger(__name__)


# NOT thread safe. Every assignment must be on the main event loop thread.
class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = MutableNotificationDict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): worker list}
    node_workers = Dict()
    # {node id hex(str): {actor id hex(str): actor table data}}
    node_actors = MutableNotificationDict()
    # {worker id(str): core worker stats}
    core_worker_stats = Dict()


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
    async def organize(cls, thread_pool_executor):
        """
        Organizes data: read from (node_physical_stats, node_stats) and updates
        (node_workers, node_worker_stats).

        This methods is not really async, but DataSource is not thread safe so we need
        to make sure it's on the main event loop thread. To avoid blocking the main
        event loop, we yield after each node processed.
        """
        loop = get_or_create_event_loop()

        node_workers = {}
        core_worker_stats = {}

        # NOTE: We copy keys of the `DataSource.nodes` to make sure
        #       it doesn't change during the iteration (since its being updated
        #       from another async task)
        for node_id in list(DataSource.nodes.keys()):
            node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
            node_stats = DataSource.node_stats.get(node_id, {})
            # Offloads the blocking operation to a thread pool executor. This also
            # yields to the event loop.
            workers = await loop.run_in_executor(
                thread_pool_executor,
                cls._extract_workers_for_node,
                node_physical_stats,
                node_stats,
            )

            for worker in workers:
                for stats in worker.get("coreWorkerStats", []):
                    worker_id = stats["workerId"]
                    core_worker_stats[worker_id] = stats

            node_workers[node_id] = workers

        DataSource.node_workers.reset(node_workers)
        DataSource.core_worker_stats.reset(core_worker_stats)

    @classmethod
    def _extract_workers_for_node(cls, node_physical_stats, node_stats):
        workers = []
        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        pid_to_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}

        for core_worker_stats in node_stats.get("coreWorkersStats", []):
            pid = core_worker_stats["pid"]

            pid_to_worker_stats[pid] = core_worker_stats
            pid_to_language[pid] = core_worker_stats["language"]
            pid_to_job_id[pid] = core_worker_stats["jobId"]

        for worker in node_physical_stats.get("workers", []):
            worker = dict(worker)
            pid = worker["pid"]

            core_worker_stats = pid_to_worker_stats.get(pid)
            # Empty list means core worker stats is not available.
            worker["coreWorkerStats"] = [core_worker_stats] if core_worker_stats else []
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

        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        death_info = node.get("deathInfo", {})
        node_info["raylet"]["stateMessage"] = compose_state_message(
            death_info.get("reason", None), death_info.get("reasonMessage", None)
        )

        if not get_summary:
            actor_table_entries = DataSource.node_actors.get(node_id, {})

            # Merge actors to node physical stats
            node_info["actors"] = {
                actor_id: await DataOrganizer._get_actor_info(actor_table_entry)
                for actor_id, actor_table_entry in actor_table_entries.items()
            }

            # Update workers to node physical stats
            node_info["workers"] = DataSource.node_workers.get(node_id, [])

        return node_info

    @classmethod
    async def get_all_node_summary(cls):
        return [
            # NOTE: We're intentionally awaiting in a loop to avoid excessive
            #       concurrency spinning up excessive # of tasks for large clusters
            await DataOrganizer.get_node_info(node_id, get_summary=True)
            for node_id in DataSource.nodes.keys()
        ]

    @classmethod
    async def get_actor_infos(cls, actor_ids: Optional[List[str]] = None):
        target_actor_table_entries: dict[str, Optional[dict]]
        if actor_ids is not None:
            target_actor_table_entries = {
                actor_id: DataSource.actors.get(actor_id) for actor_id in actor_ids
            }
        else:
            target_actor_table_entries = DataSource.actors

        return {
            actor_id: await DataOrganizer._get_actor_info(actor_table_entry)
            for actor_id, actor_table_entry in target_actor_table_entries.items()
        }

    @staticmethod
    async def _get_actor_info(actor):
        if actor is None:
            return None

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

        required_resources = parse_pg_formatted_resources_to_original(
            actor["requiredResources"]
        )
        actor["requiredResources"] = required_resources

        return actor
