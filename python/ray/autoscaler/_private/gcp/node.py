from typing import List, Optional, Tuple
import logging
import abc
import time
from uuid import uuid4
from collections import UserDict, MutableMapping
from enum import Enum, auto

from googleapiclient.discovery import Resource

from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler._private.gcp.config import MAX_POLLS, POLL_INTERVAL

logger = logging.getLogger(__name__)

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8


def _generate_node_name(labels: dict, node_suffix: str) -> str:
    name_label = labels[TAG_RAY_NODE_NAME]
    assert (len(name_label) <=
            (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 1)), (
                name_label, len(name_label))
    return f"{name_label}-{uuid4().hex[:INSTANCE_NAME_UUID_LEN]}-{node_suffix}"


def _flatten_dict(d: dict, parent_key: str = "", delimiter: str = "."):
    items = []
    for k, v in d.items():
        new_key = delimiter.join((parent_key, k)) if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(_flatten_dict(v, new_key, delimiter=delimiter).items())
        else:
            items.append((new_key, v))
    return dict(items)


class GCPNodeType(Enum):
    COMPUTE = "compute"
    TPU = "tpu"

    @staticmethod
    def from_gcp_node(node: "GCPNode"):
        if isinstance(node, GCPTPUNode):
            return GCPNodeType.TPU
        if isinstance(node, GCPComputeNode):
            return GCPNodeType.COMPUTE
        raise TypeError(f"Wrong GCPNode type {type(node)}.")

    @staticmethod
    def name_to_type(name: str):
        return GCPNodeType(name.split("-")[-1])


class GCPNode(UserDict, metaclass=abc.ABCMeta):
    """Abstraction around compute and tpu nodes"""

    NON_TERMINATED_STATUSES = None
    RUNNING_STATUSES = None
    STATUS_FIELD = None

    def __init__(self, dict: dict, resource: "GCPResource", **kwargs) -> None:
        super().__init__(dict, **kwargs)
        self.resource = resource
        assert isinstance(self.resource, GCPResource)

    def is_running(self) -> bool:
        return self.get(self.STATUS_FIELD) in self.RUNNING_STATUSES

    def is_terminated(self) -> bool:
        return self.get(self.STATUS_FIELD) not in self.NON_TERMINATED_STATUSES

    @abc.abstractmethod
    def get_tags(self) -> dict:
        return

    @abc.abstractmethod
    def get_external_ip(self) -> str:
        return

    @abc.abstractmethod
    def get_internal_ip(self) -> str:
        return

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.get('name')}>"


class GCPComputeNode(GCPNode):
    """Abstraction around compute nodes"""

    NON_TERMINATED_STATUSES = {"PROVISIONING", "STAGING", "RUNNING"}
    RUNNING_STATUSES = {"RUNNING"}
    STATUS_FIELD = "status"

    def get_tags(self) -> dict:
        return self.get("labels", {})

    def get_external_ip(self) -> str:
        return self.get("networkInterfaces", [{}])[0].get(
            "accessConfigs", [{}])[0].get("natIP", None)

    def get_internal_ip(self) -> str:
        return self.get("networkInterfaces", [{}])[0].get("networkIP")


class GCPTPUNode(GCPNode):
    """Abstraction around tpu nodes"""
    # https://cloud.google.com/tpu/docs/reference/rest/v2alpha1/projects.locations.nodes#State

    NON_TERMINATED_STATUSES = {"CREATING", "STARTING", "RESTARTING", "READY"}
    RUNNING_STATUSES = {"READY"}
    STATUS_FIELD = "state"

    def get_tags(self) -> dict:
        return self.get("labels", {})

    def get_external_ip(self) -> str:
        return self.get("networkEndpoints",
                        [{}])[0].get("accessConfig", {}).get(
                            "externalIp", None)

    def get_internal_ip(self) -> str:
        return self.get("networkEndpoints", [{}])[0].get("ipAddress", None)


class GCPResource(metaclass=abc.ABCMeta):
    """Abstraction around compute and tpu resources"""

    def __init__(self, resource: Resource, project_id: str,
                 availability_zone: str, cluster_name: str) -> None:
        self.resource = resource
        self.project_id = project_id
        self.availability_zone = availability_zone
        self.cluster_name = cluster_name

    @abc.abstractmethod
    def wait_for_operation(self, operation: dict) -> dict:
        """Waits a preset amount of time for operation to complete"""
        return None

    @abc.abstractmethod
    def list_instances(self,
                       tag_filters: Optional[dict] = None) -> List["GCPNode"]:
        """Returns a list of instances"""
        return

    @abc.abstractmethod
    def get_instance(self, node_id: str) -> "GCPNode":
        """Returns a single instance"""
        return

    @abc.abstractmethod
    def set_labels(self,
                   node: GCPNode,
                   labels: dict,
                   wait_for_operation: bool = True) -> dict:
        """Sets labels on an instance and returns result"""
        return

    @abc.abstractmethod
    def create_instance(self,
                        base_config: dict,
                        labels: dict,
                        wait_for_operation: bool = True) -> Tuple[dict, str]:
        """Creates a single instance and returns result.
        
        Returns a tuple of (result, node_name)."""
        return

    def create_instances(
            self,
            base_config: dict,
            labels: dict,
            count: int,
            wait_for_operation: bool = True) -> List[Tuple[dict, str]]:
        """Creates multiple instances and returns result.
        
        Returns a list of tuples of (result, node_name)."""
        operations = [
            self.create_instance(
                base_config, labels, wait_for_operation=False)
            for i in range(count)
        ]

        if wait_for_operation:
            results = [(self.wait_for_operation(operation), node_name)
                       for operation, node_name in operations]
        else:
            results = operations

        return results

    @abc.abstractmethod
    def delete_instance(self, node_id: str,
                        wait_for_operation: bool = True) -> dict:
        """Deletes an instance and returns result"""
        return


class GCPCompute(GCPResource):
    """Abstraction around GCP compute resource"""

    def wait_for_operation(self, operation: dict) -> dict:
        """Poll for compute zone operation until finished."""
        logger.info("wait_for_compute_zone_operation: "
                    f"Waiting for operation {operation['name']} to finish...")

        for _ in range(MAX_POLLS):
            result = self.resource.zoneOperations().get(
                project=self.project_id,
                operation=operation["name"],
                zone=self.availability_zone).execute()
            if "error" in result:
                raise Exception(result["error"])

            if result["status"] == "DONE":
                logger.info("wait_for_compute_zone_operation: "
                            f"Operation {operation['name']} finished.")
                break

            time.sleep(POLL_INTERVAL)

        return result

    def list_instances(self, tag_filters: Optional[dict] = None
                       ) -> List["GCPComputeNode"]:
        tag_filters = tag_filters or {}

        if tag_filters:
            label_filter_expr = "(" + " AND ".join([
                "(labels.{key} = {value})".format(key=key, value=value)
                for key, value in tag_filters.items()
            ]) + ")"
        else:
            label_filter_expr = ""

        instance_state_filter_expr = "(" + " OR ".join([
            "(status = {status})".format(status=status)
            for status in GCPComputeNode.NON_TERMINATED_STATUSES
        ]) + ")"

        cluster_name_filter_expr = ("(labels.{key} = {value})"
                                    "".format(
                                        key=TAG_RAY_CLUSTER_NAME,
                                        value=self.cluster_name))

        not_empty_filters = [
            f for f in [
                label_filter_expr,
                instance_state_filter_expr,
                cluster_name_filter_expr,
            ] if f
        ]

        filter_expr = " AND ".join(not_empty_filters)

        response = self.resource.instances().list(
            project=self.project_id,
            zone=self.availability_zone,
            filter=filter_expr,
        ).execute()

        instances = response.get("items", [])
        return [GCPComputeNode(i, self) for i in instances]

    def get_instance(self, node_id: str) -> "GCPComputeNode":
        instance = self.resource.instances().get(
            project=self.project_id,
            zone=self.availability_zone,
            instance=node_id,
        ).execute()

        return GCPComputeNode(instance, self)

    def set_labels(self,
                   node: GCPComputeNode,
                   labels: dict,
                   wait_for_operation: bool = True) -> dict:
        body = {
            "labels": dict(node["labels"], **labels),
            "labelFingerprint": node["labelFingerprint"]
        }
        node_id = node["name"]
        operation = self.resource.instances().setLabels(
            project=self.project_id,
            zone=self.availability_zone,
            instance=node_id,
            body=body).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result

    def create_instance(self,
                        base_config: dict,
                        labels: dict,
                        wait_for_operation: bool = True) -> Tuple[dict, str]:

        config = base_config.copy()
        # removing TPU-specific default key set in config.py
        config.pop("networkConfig", None)
        name = _generate_node_name(labels, GCPNodeType.COMPUTE.value)

        machine_type = ("zones/{zone}/machineTypes/{machine_type}"
                        "".format(
                            zone=self.availability_zone,
                            machine_type=base_config["machineType"]))
        labels = dict(config.get("labels", {}), **labels)

        config.update({
            "machineType": machine_type,
            "labels": dict(labels,
                           **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
            "name": name
        })

        operation = self.resource.instances().insert(
            project=self.project_id, zone=self.availability_zone,
            body=config).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result, name

    def delete_instance(self, node_id: str,
                        wait_for_operation: bool = True) -> dict:
        operation = self.resource.instances().delete(
            project=self.project_id,
            zone=self.availability_zone,
            instance=node_id,
        ).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result


class GCPTPU(GCPResource):
    """Abstraction around GCP TPU resource"""

    # node names already contain the path, but this is required for `parent`
    # arguments

    @property
    def path(self):
        return f"projects/{self.project_id}/locations/{self.availability_zone}"

    def wait_for_operation(self, operation: dict) -> dict:
        """Poll for TPU operation until finished."""
        logger.info("wait_for_tpu_zone_operation: "
                    f"Waiting for operation {operation['name']} to finish...")

        # TPUs take a long while to start, so we increase the MAX_POLLS
        # considerably
        max_polls = MAX_POLLS * 8

        for _ in range(max_polls):
            result = self.resource.projects().locations().operations().get(
                name=f"{operation['name']}").execute()
            if "error" in result:
                raise Exception(result["error"])

            if "response" in result:
                logger.info("wait_for_tpu_zone_operation: "
                            f"Operation {operation['name']} finished.")
                break

            time.sleep(POLL_INTERVAL)

        return result

    def list_instances(
            self, tag_filters: Optional[dict] = None) -> List["GCPTPUNode"]:
        response = self.resource.projects().locations().nodes().list(
            parent=self.path).execute()

        instances = response.get("nodes", [])
        instances = [GCPTPUNode(i, self) for i in instances]

        logger.info(f"tpu list_instances: {instances}")

        # filter_expr cannot be passed directly to API
        # so we need to filter the results ourselves

        # same logic as in GCPCompute.list_instances
        def filter_instance(instance: GCPTPUNode) -> bool:
            if instance.is_terminated():
                return False

            labels = instance.get_tags()
            if tag_filters:
                for key, value in tag_filters.items():
                    if key not in labels:
                        return False
                    if value != labels[key]:
                        return False

            return True

        instances = list(filter(filter_instance, instances))

        logger.info(f"tpu list_instances after filter: {instances}")

        return instances

    def get_instance(self, node_id: str) -> "GCPTPUNode":
        instance = self.resource.projects().locations().nodes().get(
            name=node_id).execute()

        return GCPTPUNode(instance, self)

    def set_labels(self,
                   node: GCPNode,
                   labels: dict,
                   wait_for_operation: bool = True) -> dict:
        body = {
            "labels": dict(node["labels"], **labels),
        }
        update_mask = "labels"
        logger.info(f"tpu set_labels update_mask {update_mask}")
        logger.info(f"tpu set_labels body {body}")
        operation = self.resource.projects().locations().nodes().patch(
            name=node["name"],
            updateMask=update_mask,
            body=body,
        ).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result

    def create_instance(self,
                        base_config: dict,
                        labels: dict,
                        wait_for_operation: bool = True) -> Tuple[dict, str]:
        config = base_config.copy()
        # removing Compute-specific default key set in config.py
        config.pop("networkInterfaces", None)
        name = _generate_node_name(labels, GCPNodeType.TPU.value)

        labels = dict(config.get("labels", {}), **labels)

        config.update({
            "labels": dict(labels,
                           **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
            # this is required for SSH to work
            "networkConfig": {
                "enableExternalIps": True
            }
        })

        operation = self.resource.projects().locations().nodes().create(
            parent=self.path,
            body=config,
            nodeId=name,
        ).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result, name

    def delete_instance(self, node_id: str,
                        wait_for_operation: bool = True) -> dict:
        operation = self.resource.projects().locations().nodes().delete(
            name=node_id).execute()

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result
