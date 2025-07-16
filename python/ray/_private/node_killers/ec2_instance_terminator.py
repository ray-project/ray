import logging
import threading
import time
from typing import Set

import boto3
import requests

import ray
import ray._private.services as services
from .node_killer_base import NodeKillerBase

logger = logging.getLogger(__name__)

# Log termination events to a file.
file_handler = logging.FileHandler("/tmp/ray/node_killer.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)


@ray.remote(num_cpus=0)
class EC2InstanceTerminator(NodeKillerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        region_name = self._get_region_name()
        self._ec2 = boto3.client("ec2", region_name=region_name)

    def _get_region_name(self):
        """Get the name of the region that the instance is in."""
        try:
            token = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                timeout=1,
            ).text
            region_name = requests.get(
                "http://169.254.169.254/latest/meta-data/placement/region",
                headers={"X-aws-ec2-metadata-token": token},
                timeout=1,
            ).text
            return region_name
        except requests.exceptions.ConnectTimeout as e:
            logger.error(f"Failed to get region name, {e=}")
            raise RuntimeError(
                f"Failed to get region name, {e=}. This can happen if you're not "
                "running this actor on an EC2 instance."
            )

    def _kill_resource(self, node_id, node_to_kill_ip, _):
        if node_to_kill_ip is not None:
            self._terminate_ec2_instance(node_to_kill_ip)
            self.killed.add(node_id)

    def _terminate_ec2_instance(self, private_ip: str) -> None:
        logger.debug(f"Terminating instance, {private_ip=}")
        instance_id = self._get_instance_id(private_ip)
        response = self._ec2.terminate_instances(InstanceIds=[instance_id])

        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.error(f"Failed to terminate instance, {private_ip=}, {response=}")
        else:
            logger.info(f"Terminated instance, {private_ip=}")

    def _get_instance_id(self, private_ip: str) -> str:
        """Get the instance ID for a given private IP address."""
        response = self._ec2.describe_instances(
            Filters=[
                {"Name": "private-ip-address", "Values": [private_ip]},
            ]
        )

        instance_ids = [
            instance["InstanceId"]
            for reservation in response["Reservations"]
            for instance in reservation["Instances"]
        ]
        assert (
            len(instance_ids) == 1
        ), f"Expected 1 instance with {private_ip=}, got {len(instance_ids)}"

        return instance_ids[0]


@ray.remote(num_cpus=0)
class EC2InstanceTerminatorWithGracePeriod(EC2InstanceTerminator):
    def __init__(self, *args, grace_period_s: int = 30, **kwargs):
        super().__init__(*args, **kwargs)

        self._grace_period_s = grace_period_s
        self._kill_threads: Set[threading.Thread] = set()

    def _kill_resource(self, node_id, node_to_kill_ip, _):
        assert node_id not in self.killed

        # Clean up any completed threads.
        for thread in self._kill_threads.copy():
            if not thread.is_alive():
                thread.join()
                self._kill_threads.remove(thread)

        def _kill_node_with_grace_period(node_id, node_to_kill_ip):
            self._drain_node(node_id)
            time.sleep(self._grace_period_s)
            self._terminate_ec2_instance(node_to_kill_ip)

        logger.info(f"Starting killing thread {node_id=}, {node_to_kill_ip=}")
        thread = threading.Thread(
            target=_kill_node_with_grace_period,
            args=(node_id, node_to_kill_ip),
            daemon=True,
        )
        thread.start()
        self._kill_threads.add(thread)
        self.killed.add(node_id)

    def _drain_node(self, node_id: str) -> None:
        # We need to lazily import this object. Otherwise, Ray can't serialize the
        # class.
        from ray.core.generated import autoscaler_pb2

        assert ray.NodeID.from_hex(node_id) != ray.NodeID.nil()

        logger.info(f"Draining node {node_id=}")
        address = services.canonicalize_bootstrap_address_or_die(addr="auto")
        gcs_client = ray._raylet.GcsClient(address=address)
        deadline_timestamp_ms = (time.time_ns() // 1e6) + (self._grace_period_s * 1e3)

        try:
            is_accepted, _ = gcs_client.drain_node(
                node_id,
                autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
                "",
                deadline_timestamp_ms,
            )
        except ray.exceptions.RayError as e:
            logger.error(f"Failed to drain node {node_id=}")
            raise e

        assert is_accepted, "Drain node request was rejected"

    def _cleanup(self):
        for thread in self._kill_threads.copy():
            thread.join()
            self._kill_threads.remove(thread)

        assert not self._kill_threads
