"""Abstractions around GCP resources and nodes.

The logic has been abstracted away here to allow for different GCP resources
(API endpoints), which can differ widely, making it impossible to use
the same logic for everything.

Classes inheriting from ``GCPResource`` represent different GCP resources -
API endpoints that allow for nodes to be created, removed, listed and
otherwise managed. Those classes contain methods abstracting GCP REST API
calls.
Each resource has a corresponding node type, represented by a
class inheriting from ``GCPNode``. Those classes are essentially dicts
with some extra methods. The instances of those classes will be created
from API responses.

The ``GCPNodeType`` enum is a lightweight way to classify nodes.

Currently, Compute and TPU resources & nodes are supported.

In order to add support for new resources, create classes inheriting from
``GCPResource`` and ``GCPNode``, update the ``GCPNodeType`` enum,
update the ``_generate_node_name`` method and finally update the
node provider.
"""

import abc
import logging
import re
import time
from collections import UserDict
from copy import deepcopy
from enum import Enum
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from google_auth_httplib2 import AuthorizedHttp
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

logger = logging.getLogger(__name__)

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 8
MAX_POLLS = 12
# TPUs take a long while to respond, so we increase the MAX_POLLS
# considerably - this probably could be smaller
# TPU deletion uses MAX_POLLS
MAX_POLLS_TPU = MAX_POLLS * 8
POLL_INTERVAL = 5


def _retry_on_exception(
    exception: Union[Exception, Tuple[Exception]],
    regex: Optional[str] = None,
    max_retries: int = MAX_POLLS,
    retry_interval_s: int = POLL_INTERVAL,
):
    """Retry a function call n-times for as long as it throws an exception."""

    def dec(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def try_catch_exc():
                try:
                    value = func(*args, **kwargs)
                    return value
                except Exception as e:
                    if not isinstance(e, exception) or (
                        regex and not re.search(regex, str(e))
                    ):
                        raise e
                    return e

            for _ in range(max_retries):
                ret = try_catch_exc()
                if not isinstance(ret, Exception):
                    break
                time.sleep(retry_interval_s)
            if isinstance(ret, Exception):
                raise ret
            return ret

        return wrapper

    return dec


def _generate_node_name(labels: dict, node_suffix: str) -> str:
    """Generate node name from labels and suffix.

    This is required so that the correct resource can be selected
    when the only information autoscaler has is the name of the node.

    The suffix is expected to be one of 'compute' or 'tpu'
    (as in ``GCPNodeType``).
    """
    name_label = labels[TAG_RAY_NODE_NAME]
    assert len(name_label) <= (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 1), (
        name_label,
        len(name_label),
    )
    return f"{name_label}-{uuid4().hex[:INSTANCE_NAME_UUID_LEN]}-{node_suffix}"


class GCPNodeType(Enum):
    """Enum for GCP node types (compute & tpu)"""

    COMPUTE = "compute"
    TPU = "tpu"

    @staticmethod
    def from_gcp_node(node: "GCPNode"):
        """Return GCPNodeType based on ``node``'s class"""
        if isinstance(node, GCPTPUNode):
            return GCPNodeType.TPU
        if isinstance(node, GCPComputeNode):
            return GCPNodeType.COMPUTE
        raise TypeError(f"Wrong GCPNode type {type(node)}.")

    @staticmethod
    def name_to_type(name: str):
        """Provided a node name, determine the type.

        This expects the name to be in format '[NAME]-[UUID]-[TYPE]',
        where [TYPE] is either 'compute' or 'tpu'.
        """
        return GCPNodeType(name.split("-")[-1])


class GCPNode(UserDict, metaclass=abc.ABCMeta):
    """Abstraction around compute and tpu nodes"""

    NON_TERMINATED_STATUSES = None
    RUNNING_STATUSES = None
    STATUS_FIELD = None

    def __init__(self, base_dict: dict, resource: "GCPResource", **kwargs) -> None:
        super().__init__(base_dict, **kwargs)
        self.resource = resource
        assert isinstance(self.resource, GCPResource)

    def is_running(self) -> bool:
        return self.get(self.STATUS_FIELD) in self.RUNNING_STATUSES

    def is_terminated(self) -> bool:
        return self.get(self.STATUS_FIELD) not in self.NON_TERMINATED_STATUSES

    @abc.abstractmethod
    def get_labels(self) -> dict:
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

    # https://cloud.google.com/compute/docs/instances/instance-life-cycle
    NON_TERMINATED_STATUSES = {"PROVISIONING", "STAGING", "RUNNING"}
    TERMINATED_STATUSES = {"TERMINATED", "SUSPENDED"}
    RUNNING_STATUSES = {"RUNNING"}
    STATUS_FIELD = "status"

    def get_labels(self) -> dict:
        return self.get("labels", {})

    def get_external_ip(self) -> str:
        return (
            self.get("networkInterfaces", [{}])[0]
            .get("accessConfigs", [{}])[0]
            .get("natIP", None)
        )

    def get_internal_ip(self) -> str:
        return self.get("networkInterfaces", [{}])[0].get("networkIP")


class GCPTPUNode(GCPNode):
    """Abstraction around tpu nodes"""

    # https://cloud.google.com/tpu/docs/reference/rest/v2alpha1/projects.locations.nodes#State

    NON_TERMINATED_STATUSES = {"CREATING", "STARTING", "RESTARTING", "READY"}
    RUNNING_STATUSES = {"READY"}
    STATUS_FIELD = "state"

    def get_labels(self) -> dict:
        return self.get("labels", {})

    @property
    def num_workers(self) -> int:
        return len(self.get("networkEndpoints", [{}]))

    def get_external_ips(self) -> List[str]:
        return self.get("networkEndpoints", [{}])

    def get_external_ip(self, worker_index: int = 0) -> str:
        return (
            self.get_external_ips()[worker_index]
            .get("accessConfig", {})
            .get("externalIp", None)
        )

    def get_internal_ips(self) -> List[str]:
        return self.get("networkEndpoints", [{}])

    def get_internal_ip(self, worker_index: int = 0) -> str:
        return self.get_internal_ips()[worker_index].get("ipAddress", None)


class GCPResource(metaclass=abc.ABCMeta):
    """Abstraction around compute and TPU resources"""

    def __init__(
        self,
        resource: Resource,
        project_id: str,
        availability_zone: str,
        cluster_name: str,
    ) -> None:
        self.resource = resource
        self.project_id = project_id
        self.availability_zone = availability_zone
        self.cluster_name = cluster_name

    @abc.abstractmethod
    def get_new_authorized_http(self, http: AuthorizedHttp) -> AuthorizedHttp:
        """Generate a new AuthorizedHttp object with the given credentials."""
        return

    @abc.abstractmethod
    def wait_for_operation(
        self,
        operation: dict,
        max_polls: int = MAX_POLLS,
        poll_interval: int = POLL_INTERVAL,
    ) -> dict:
        """Waits a preset amount of time for operation to complete."""
        return None

    @abc.abstractmethod
    def list_instances(
        self,
        label_filters: Optional[dict] = None,
        is_terminated: bool = False,
    ) -> List["GCPNode"]:
        """Returns a filtered list of all instances.

        The filter removes all terminated instances and, if ``label_filters``
        are provided, all instances which labels are not matching the
        ones provided.
        """
        return

    @abc.abstractmethod
    def get_instance(self, node_id: str) -> "GCPNode":
        """Returns a single instance."""
        return

    @abc.abstractmethod
    def set_labels(
        self, node: GCPNode, labels: dict, wait_for_operation: bool = True
    ) -> dict:
        """Sets labels on an instance and returns result.

        Completely replaces the labels dictionary."""
        return

    @abc.abstractmethod
    def create_instance(
        self, base_config: dict, labels: dict, wait_for_operation: bool = True
    ) -> Tuple[dict, str]:
        """Creates a single instance and returns result.

        Returns a tuple of (result, node_name).
        """
        return

    def create_instances(
        self,
        base_config: dict,
        labels: dict,
        count: int,
        wait_for_operation: bool = True,
    ) -> List[Tuple[dict, str]]:
        """Creates multiple instances and returns result.

        Returns a list of tuples of (result, node_name).
        """
        operations = [
            self.create_instance(base_config, labels, wait_for_operation=False)
            for i in range(count)
        ]

        if wait_for_operation:
            results = [
                (self.wait_for_operation(operation), node_name)
                for operation, node_name in operations
            ]
        else:
            results = operations

        return results

    @abc.abstractmethod
    def delete_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        """Deletes an instance and returns result."""
        return

    @abc.abstractmethod
    def stop_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        """Deletes an instance and returns result."""
        return

    @abc.abstractmethod
    def start_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        """Starts a single instance and returns result."""
        return


class GCPCompute(GCPResource):
    """Abstraction around GCP compute resource"""

    def get_new_authorized_http(self, http: AuthorizedHttp) -> AuthorizedHttp:
        """Generate a new AuthorizedHttp object with the given credentials."""
        new_http = AuthorizedHttp(http.credentials)
        return new_http

    def wait_for_operation(
        self,
        operation: dict,
        max_polls: int = MAX_POLLS,
        poll_interval: int = POLL_INTERVAL,
    ) -> dict:
        """Poll for compute zone operation until finished."""
        logger.info(
            "wait_for_compute_zone_operation: "
            f"Waiting for operation {operation['name']} to finish..."
        )

        for _ in range(max_polls):
            result = (
                self.resource.zoneOperations()
                .get(
                    project=self.project_id,
                    operation=operation["name"],
                    zone=self.availability_zone,
                )
                .execute(http=self.get_new_authorized_http(self.resource._http))
            )
            if "error" in result:
                raise Exception(result["error"])

            if result["status"] == "DONE":
                logger.info(
                    "wait_for_compute_zone_operation: "
                    f"Operation {operation['name']} finished."
                )
                break

            time.sleep(poll_interval)

        return result

    def list_instances(
        self,
        label_filters: Optional[dict] = None,
        is_terminated: bool = False,
    ) -> List[GCPComputeNode]:
        label_filters = label_filters or {}

        if label_filters:
            label_filter_expr = (
                "("
                + " AND ".join(
                    [
                        "(labels.{key} = {value})".format(key=key, value=value)
                        for key, value in label_filters.items()
                    ]
                )
                + ")"
            )
        else:
            label_filter_expr = ""

        statuses = (
            GCPComputeNode.TERMINATED_STATUSES
            if is_terminated
            else GCPComputeNode.NON_TERMINATED_STATUSES
        )

        instance_state_filter_expr = (
            "("
            + " OR ".join(
                ["(status = {status})".format(status=status) for status in statuses]
            )
            + ")"
        )

        cluster_name_filter_expr = "(labels.{key} = {value})".format(
            key=TAG_RAY_CLUSTER_NAME, value=self.cluster_name
        )

        # TPU VMs spawn accompanying Compute Instances that must be filtered out,
        # else this results in duplicated nodes.
        tpu_negation_filter_expr = "(NOT labels.{label}:*)".format(label="tpu_cores")

        not_empty_filters = [
            f
            for f in [
                label_filter_expr,
                instance_state_filter_expr,
                cluster_name_filter_expr,
                tpu_negation_filter_expr,
            ]
            if f
        ]

        filter_expr = " AND ".join(not_empty_filters)

        response = (
            self.resource.instances()
            .list(
                project=self.project_id,
                zone=self.availability_zone,
                filter=filter_expr,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        instances = response.get("items", [])
        return [GCPComputeNode(i, self) for i in instances]

    def get_instance(self, node_id: str) -> GCPComputeNode:
        instance = (
            self.resource.instances()
            .get(
                project=self.project_id,
                zone=self.availability_zone,
                instance=node_id,
            )
            .execute()
        )

        return GCPComputeNode(instance, self)

    def set_labels(
        self, node: GCPComputeNode, labels: dict, wait_for_operation: bool = True
    ) -> dict:
        body = {
            "labels": dict(node["labels"], **labels),
            "labelFingerprint": node["labelFingerprint"],
        }
        node_id = node["name"]
        operation = (
            self.resource.instances()
            .setLabels(
                project=self.project_id,
                zone=self.availability_zone,
                instance=node_id,
                body=body,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result

    def _convert_resources_to_urls(
        self, configuration_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Ensures that resources are in their full URL form.

        GCP expects machineType and acceleratorType to be a full URL (e.g.
        `zones/us-west1/machineTypes/n1-standard-2`) instead of just the
        type (`n1-standard-2`)

        Args:
            configuration_dict: Dict of options that will be passed to GCP
        Returns:
            Input dictionary, but with possibly expanding `machineType` and
                `acceleratorType`.
        """
        configuration_dict = deepcopy(configuration_dict)
        existing_machine_type = configuration_dict["machineType"]
        if not re.search(".*/machineTypes/.*", existing_machine_type):
            configuration_dict[
                "machineType"
            ] = "zones/{zone}/machineTypes/{machine_type}".format(
                zone=self.availability_zone,
                machine_type=configuration_dict["machineType"],
            )

        for accelerator in configuration_dict.get("guestAccelerators", []):
            gpu_type = accelerator["acceleratorType"]
            if not re.search(".*/acceleratorTypes/.*", gpu_type):
                accelerator[
                    "acceleratorType"
                ] = "projects/{project}/zones/{zone}/acceleratorTypes/{accelerator}".format(  # noqa: E501
                    project=self.project_id,
                    zone=self.availability_zone,
                    accelerator=gpu_type,
                )

        return configuration_dict

    def create_instance(
        self, base_config: dict, labels: dict, wait_for_operation: bool = True
    ) -> Tuple[dict, str]:

        config = self._convert_resources_to_urls(base_config)
        # removing TPU-specific default key set in config.py
        config.pop("networkConfig", None)
        name = _generate_node_name(labels, GCPNodeType.COMPUTE.value)

        labels = dict(config.get("labels", {}), **labels)

        config.update(
            {
                "labels": dict(labels, **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
                "name": name,
            }
        )

        # Allow Google Compute Engine instance templates.
        #
        # Config example:
        #
        #     ...
        #     node_config:
        #         sourceInstanceTemplate: global/instanceTemplates/worker-16
        #         machineType: e2-standard-16
        #     ...
        #
        # node_config parameters override matching template parameters, if any.
        #
        # https://cloud.google.com/compute/docs/instance-templates
        # https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
        source_instance_template = config.pop("sourceInstanceTemplate", None)

        operation = (
            self.resource.instances()
            .insert(
                project=self.project_id,
                zone=self.availability_zone,
                sourceInstanceTemplate=source_instance_template,
                body=config,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result, name

    def delete_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        operation = (
            self.resource.instances()
            .delete(
                project=self.project_id,
                zone=self.availability_zone,
                instance=node_id,
            )
            .execute()
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result

    def stop_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        operation = (
            self.resource.instances()
            .stop(
                project=self.project_id,
                zone=self.availability_zone,
                instance=node_id,
            )
            .execute()
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation
        return result

    def start_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:

        operation = (
            self.resource.instances()
            .start(
                project=self.project_id,
                zone=self.availability_zone,
                instance=node_id,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

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

    def get_new_authorized_http(self, http: AuthorizedHttp) -> AuthorizedHttp:
        """Generate a new AuthorizedHttp object with the given credentials."""
        new_http = AuthorizedHttp(http.credentials)
        return new_http

    def wait_for_operation(
        self,
        operation: dict,
        max_polls: int = MAX_POLLS_TPU,
        poll_interval: int = POLL_INTERVAL,
    ) -> dict:
        """Poll for TPU operation until finished."""
        logger.info(
            "wait_for_tpu_operation: "
            f"Waiting for operation {operation['name']} to finish..."
        )

        for _ in range(max_polls):
            result = (
                self.resource.projects()
                .locations()
                .operations()
                .get(name=f"{operation['name']}")
                .execute(http=self.get_new_authorized_http(self.resource._http))
            )
            if "error" in result:
                raise Exception(result["error"])

            if "response" in result:
                logger.info(
                    "wait_for_tpu_operation: "
                    f"Operation {operation['name']} finished."
                )
                break

            time.sleep(poll_interval)

        return result

    def list_instances(
        self,
        label_filters: Optional[dict] = None,
        is_terminated: bool = False,
    ) -> List[GCPTPUNode]:
        response = (
            self.resource.projects()
            .locations()
            .nodes()
            .list(parent=self.path)
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        instances = response.get("nodes", [])
        instances = [GCPTPUNode(i, self) for i in instances]

        # filter_expr cannot be passed directly to API
        # so we need to filter the results ourselves

        # same logic as in GCPCompute.list_instances
        label_filters = label_filters or {}
        label_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        def filter_instance(instance: GCPTPUNode) -> bool:
            if instance.is_terminated():
                return False

            labels = instance.get_labels()
            if label_filters:
                for key, value in label_filters.items():
                    if key not in labels:
                        return False
                    if value != labels[key]:
                        return False

            return True

        instances = list(filter(filter_instance, instances))

        return instances

    def get_instance(self, node_id: str) -> GCPTPUNode:
        instance = (
            self.resource.projects()
            .locations()
            .nodes()
            .get(name=node_id)
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        return GCPTPUNode(instance, self)

    # this sometimes fails without a clear reason, so we retry it
    # MAX_POLLS times
    @_retry_on_exception(HttpError, "unable to queue the operation")
    def set_labels(
        self, node: GCPTPUNode, labels: dict, wait_for_operation: bool = True
    ) -> dict:
        body = {
            "labels": dict(node["labels"], **labels),
        }
        update_mask = "labels"

        operation = (
            self.resource.projects()
            .locations()
            .nodes()
            .patch(
                name=node["name"],
                updateMask=update_mask,
                body=body,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result

    def create_instance(
        self, base_config: dict, labels: dict, wait_for_operation: bool = True
    ) -> Tuple[dict, str]:
        config = base_config.copy()
        # removing Compute-specific default key set in config.py
        config.pop("networkInterfaces", None)
        name = _generate_node_name(labels, GCPNodeType.TPU.value)

        labels = dict(config.get("labels", {}), **labels)

        config.update(
            {
                "labels": dict(labels, **{TAG_RAY_CLUSTER_NAME: self.cluster_name}),
            }
        )

        if "networkConfig" not in config:
            config["networkConfig"] = {}
        if "enableExternalIps" not in config["networkConfig"]:
            # this is required for SSH to work, per google documentation
            # https://cloud.google.com/tpu/docs/users-guide-tpu-vm#create-curl
            config["networkConfig"]["enableExternalIps"] = True

        # replace serviceAccounts with serviceAccount, and scopes with scope
        # this is necessary for the head node to work
        # see here: https://tpu.googleapis.com/$discovery/rest?version=v2alpha1
        if "serviceAccounts" in config:
            config["serviceAccount"] = config.pop("serviceAccounts")[0]
            config["serviceAccount"]["scope"] = config["serviceAccount"].pop("scopes")

        operation = (
            self.resource.projects()
            .locations()
            .nodes()
            .create(
                parent=self.path,
                body=config,
                nodeId=name,
            )
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation)
        else:
            result = operation

        return result, name

    def delete_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        operation = (
            self.resource.projects()
            .locations()
            .nodes()
            .delete(name=node_id)
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        # No need to increase MAX_POLLS for deletion
        if wait_for_operation:
            result = self.wait_for_operation(operation, max_polls=MAX_POLLS)
        else:
            result = operation

        return result

    def stop_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        operation = (
            self.resource.projects()
            .locations()
            .nodes()
            .stop(name=node_id)
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation, max_polls=MAX_POLLS)
        else:
            result = operation

        return result

    def start_instance(self, node_id: str, wait_for_operation: bool = True) -> dict:
        operation = (
            self.resource.projects()
            .locations()
            .nodes()
            .start(name=node_id)
            .execute(http=self.get_new_authorized_http(self.resource._http))
        )

        if wait_for_operation:
            result = self.wait_for_operation(operation, max_polls=MAX_POLLS)
        else:
            result = operation

        return result
