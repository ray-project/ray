import os
import ray
import asyncio
import logging
from typing import List, Union
from dataclasses import dataclass
import requests
import anyscale
import time
import argparse
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


@dataclass
class ClusterComputeConfigUpdate:
    # Path to the field that needs to be updated
    field_path: List[Union[str, int]]
    # Field value to be updated to
    field_value: Union[str, int]
    # When to update the field in terms of seconds since run of the updater
    update_time_seconds_since_run: int

    def __init__(self, update: str):
        field_path, field_value, update_time_seconds_since_run = update.split(":")

        self.field_path = []
        for field in field_path.split("."):
            if field.isdigit():
                self.field_path.append(int(field))
            else:
                self.field_path.append(field)

        if field_value.isdigit():
            self.field_value = int(field_value)
        else:
            self.field_value = field_value

        self.update_time_seconds_since_run = int(update_time_seconds_since_run)


class ClusterComputeConfig:
    def __init__(self):
        self.cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
        sdk = anyscale.AnyscaleSDK()
        self.cluster = sdk.get_cluster(self.cluster_id)
        self.compute_config_dict = anyscale.compute_config.get(
            name="", _id=self.cluster.result.cluster_compute_id
        ).config.to_dict()
        logging.info(
            f"Fetched compute config {self.compute_config_dict} "
            f"for cluster {self.cluster}"
        )

    def update(self, field_path: List[Union[str, int]], field_value: Union[str, int]):
        current = self.compute_config_dict
        for field in field_path[:-1]:
            current = current[field]
        current[field_path[-1]] = field_value

        new_compute_config = anyscale.compute_config.models.ComputeConfig.from_dict(
            self.compute_config_dict
        )
        new_compute_config_name = anyscale.compute_config.create(
            new_compute_config, name=None
        )
        new_compute_config_id = anyscale.compute_config.get(
            name=new_compute_config_name
        ).id

        response = requests.put(
            f"https://console.anyscale-staging.com/api/v2/sessions/{self.cluster_id}/"
            "cluster_config_with_session_idle_timeout",
            params={
                "build_id": self.cluster.result.cluster_environment_build_id,
                "compute_template_id": new_compute_config_id,
            },
            headers={"Authorization": f"Bearer {os.environ['ANYSCALE_CLI_TOKEN']}"},
        )

        logging.info(
            f"Update compute config to {self.compute_config_dict}, "
            f"got response {response}"
        )
        response.raise_for_status()


@ray.remote(num_cpus=0)
class ClusterComputeConfigUpdater:
    def __init__(self, updates: List[ClusterComputeConfigUpdate]):
        self.cluster_compute_config = ClusterComputeConfig()
        self.updates = updates
        self.start_time = None

    async def run(self):
        logging.info("Start to run")
        self.start_time = time.monotonic()
        while self.updates:
            delay = (
                self.start_time + self.updates[0].update_time_seconds_since_run
            ) - time.monotonic()
            if delay > 0:
                logging.info(f"Sleep for {delay} seconds")
                await asyncio.sleep(delay)
            self.cluster_compute_config.update(
                self.updates[0].field_path, self.updates[0].field_value
            )
            self.updates.pop(0)

    async def wait_until_run(self):
        while not self.start_time:
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    ray.init(logging_config=ray.LoggingConfig(encoding="TEXT"))
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "--updates",
        type=str,
        nargs="+",
        help=(
            "A list of updates, each update has format: "
            "field_path:field_value:update_time_seconds_since_run."
            "field_path is a dot separated list of fields from root "
            "to the target field to be updated, e.g. worker_nodes.0.max_nodes."
        ),
    )

    args = arg_parser.parse_args()

    updates = [ClusterComputeConfigUpdate(update) for update in args.updates]
    logging.info(f"Compute config updates are {updates}")

    head_node_id = ray.get_runtime_context().get_node_id()
    updater = ClusterComputeConfigUpdater.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        namespace="release_test_namespace",
        name="ClusterComputeConfigUpdater",
        lifetime="detached",
    ).remote(updates)
    updater.run.remote()
    ray.get(updater.wait_until_run.remote())
