import logging

import ray.new_dashboard.consts as dashboard_consts
import ray.new_dashboard.memory as memory
import asyncio
from ray.new_dashboard.utils import Dict, Signal

logger = logging.getLogger(__name__)


class GlobalSignals:
    node_info_fetched = Signal(dashboard_consts.SIGNAL_NODE_INFO_FETCHED)


class DataSource:
    # {node id hex(str): node stats(dict of GetNodeStatsReply
    # in node_manager.proto)}
    node_stats = Dict()
    # {node id hex(str): node physical stats(dict from reporter_agent.py)}
    node_physical_stats = Dict()
    # {actor id hex(str): actor table data(dict of ActorTableData
    # in gcs.proto)}
    actors = Dict()
    # {node id hex(str): dashboard agent [http port(int), grpc port(int)]}
    agents = Dict()
    # {node id hex(str): gcs node info(dict of GcsNodeInfo in gcs.proto)}
    nodes = Dict()
    # {node id hex(str): ip address(str)}
    node_id_to_ip = Dict()
    # {node id hex(str): hostname(str)}
    node_id_to_hostname = Dict()


class DataOrganizer:
    @staticmethod
    async def purge():
        # Purge data that is out of date.
        # These data sources are maintained by DashboardHead,
        # we do not needs to purge them:
        #   * agents
        #   * nodes
        #   * node_id_to_ip
        #   * node_id_to_hostname
        logger.info("Purge data.")
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
    async def get_node_actors(cls, node_id):
        node_stats = DataSource.node_stats.get(node_id, {})
        node_worker_id_set = set()
        for worker_stats in node_stats.get("workersStats", []):
            node_worker_id_set.add(worker_stats["workerId"])
        node_actors = {}
        for actor_id, actor_table_data in DataSource.actors.items():
            if actor_table_data["address"]["workerId"] in node_worker_id_set:
                node_actors[actor_id] = actor_table_data
        return node_actors

    @classmethod
    async def get_node_info(cls, node_id):
        node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
        node_stats = DataSource.node_stats.get(node_id, {})
        node = DataSource.nodes.get(node_id, {})

        # Merge coreWorkerStats (node stats) to workers (node physical stats)
        workers_stats = node_stats.pop("workersStats", {})
        pid_to_worker_stats = {}
        pid_to_language = {}
        pid_to_job_id = {}
        for stats in workers_stats:
            d = pid_to_worker_stats.setdefault(stats["pid"], {}).setdefault(
                stats["workerId"], stats["coreWorkerStats"])
            d["workerId"] = stats["workerId"]
            pid_to_language.setdefault(stats["pid"],
                                       stats.get("language", "PYTHON"))
            pid_to_job_id.setdefault(stats["pid"],
                                     stats["coreWorkerStats"]["jobId"])

        for worker in node_physical_stats.get("workers", []):
            worker_stats = pid_to_worker_stats.get(worker["pid"], {})
            worker["coreWorkerStats"] = list(worker_stats.values())
            worker["language"] = pid_to_language.get(worker["pid"], "")
            worker["jobId"] = pid_to_job_id.get(worker["pid"], "ffff")

        node_info = node_physical_stats
        # Merge node stats to node physical stats
        node_info["raylet"] = node_stats
        # Merge GcsNodeInfo to node physical stats
        node_info["raylet"].update(node)
        # Merge actors to node physical stats
        node_info["actors"] = await cls.get_node_actors(node_id)

        await GlobalSignals.node_info_fetched.send(node_info)

        return node_info

    @classmethod
    async def get_all_node_summary(cls):
        all_nodes_summary = []
        for node_id in DataSource.nodes.keys():
            node_info = await cls.get_node_info(node_id)
            node_info.pop("workers", None)
            node_info.pop("actors", None)
            node_info["raylet"].pop("workersStats", None)
            node_info["raylet"].pop("viewData", None)
            all_nodes_summary.append(node_info)
        return all_nodes_summary
    
    @classmethod
    async def get_all_node_details(cls):
        node_details = []
        for node_id in DataSource.nodes.keys():
            node_details.append(await cls.get_node_info(node_id))
        return node_details

    @classmethod
    async def get_all_actors(cls):
        actors = []
        for node_id in DataSource.nodes.keys():
            actors.append(await cls.get_node_actors(node_id))
        return actors

    @classmethod
    async def get_memory_table(cls, sort_by=memory.SortingType.OBJECT_SIZE, group_by=memory.GroupByType.STACK_TRACE):
        all_worker_stats = []
        for node_stats in DataSource.node_stats.values():
            all_worker_stats.extend(node_stats.get("workersStats", []))
        memory_information = memory.construct_memory_table(all_worker_stats, group_by=group_by, sort_by=sort_by)
        return memory_information
