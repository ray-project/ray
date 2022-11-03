import copy
import logging
import math
import re

from kubernetes import client
from kubernetes.client.rest import ApiException

import ray._private.ray_constants as ray_constants
from ray.autoscaler._private._kubernetes import auth_api, core_api, log_prefix

logger = logging.getLogger(__name__)

MEMORY_SIZE_UNITS = {
    "K": 2**10,
    "M": 2**20,
    "G": 2**30,
    "T": 2**40,
    "P": 2**50,
}


class InvalidNamespaceError(ValueError):
    def __init__(self, field_name, namespace):
        self.message = (
            "Namespace of {} config doesn't match provided "
            "namespace '{}'. Either set it to {} or remove the "
            "field".format(field_name, namespace, namespace)
        )

    def __str__(self):
        return self.message


def using_existing_msg(resource_type, name):
    return "using existing {} '{}'".format(resource_type, name)


def updating_existing_msg(resource_type, name):
    return "updating existing {} '{}'".format(resource_type, name)


def not_found_msg(resource_type, name):
    return "{} '{}' not found, attempting to create it".format(resource_type, name)


def not_checking_msg(resource_type, name):
    return "not checking if {} '{}' exists".format(resource_type, name)


def created_msg(resource_type, name):
    return "successfully created {} '{}'".format(resource_type, name)


def not_provided_msg(resource_type):
    return "no {} config provided, must already exist".format(resource_type)


def bootstrap_kubernetes(config):
    if not config["provider"]["use_internal_ips"]:
        return ValueError(
            "Exposing external IP addresses for ray containers isn't "
            "currently supported. Please set "
            "'use_internal_ips' to false."
        )

    if config["provider"].get("_operator"):
        namespace = config["provider"]["namespace"]
    else:
        namespace = _configure_namespace(config["provider"])

    _configure_services(namespace, config["provider"])

    if not config["provider"].get("_operator"):
        # These steps are unecessary when using the Operator.
        _configure_autoscaler_service_account(namespace, config["provider"])
        _configure_autoscaler_role(namespace, config["provider"])
        _configure_autoscaler_role_binding(namespace, config["provider"])

    return config


def fillout_resources_kubernetes(config):
    """Fills CPU and GPU resources by reading pod spec of each available node
    type.

    For each node type and each of CPU/GPU, looks at container's resources
    and limits, takes min of the two. The result is rounded up, as Ray does
    not currently support fractional CPU.
    """
    if "available_node_types" not in config:
        return config
    node_types = copy.deepcopy(config["available_node_types"])
    head_node_type = config["head_node_type"]
    for node_type in node_types:

        node_config = node_types[node_type]["node_config"]
        # The next line is for compatibility with configs like
        # kubernetes/example-ingress.yaml,
        # cf. KubernetesNodeProvider.create_node().
        pod = node_config.get("pod", node_config)
        container_data = pod["spec"]["containers"][0]

        autodetected_resources = get_autodetected_resources(container_data)
        if node_types == head_node_type:
            # we only autodetect worker type node memory resource
            autodetected_resources.pop("memory")
        if "resources" not in config["available_node_types"][node_type]:
            config["available_node_types"][node_type]["resources"] = {}
        autodetected_resources.update(
            config["available_node_types"][node_type]["resources"]
        )
        config["available_node_types"][node_type]["resources"] = autodetected_resources
        logger.debug(
            "Updating the resources of node type {} to include {}.".format(
                node_type, autodetected_resources
            )
        )
    return config


def get_autodetected_resources(container_data):
    container_resources = container_data.get("resources", None)
    if container_resources is None:
        return {"CPU": 0, "GPU": 0}

    node_type_resources = {
        resource_name.upper(): get_resource(container_resources, resource_name)
        for resource_name in ["cpu", "gpu"]
    }

    memory_limits = get_resource(container_resources, "memory")
    node_type_resources["memory"] = int(
        memory_limits * (1 - ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION)
    )

    return node_type_resources


def get_resource(container_resources, resource_name):
    limit = _get_resource(container_resources, resource_name, field_name="limits")
    # float("inf") means there's no limit set
    return 0 if limit == float("inf") else int(limit)


def _get_resource(container_resources, resource_name, field_name):
    """Returns the resource quantity.

    The amount of resource is rounded up to nearest integer.
    Returns float("inf") if the resource is not present.

    Args:
        container_resources: Container's resource field.
        resource_name: One of 'cpu', 'gpu' or memory.
        field_name: One of 'requests' or 'limits'.

    Returns:
        Union[int, float]: Detected resource quantity.
    """
    if field_name not in container_resources:
        # No limit/resource field.
        return float("inf")
    resources = container_resources[field_name]
    # Look for keys containing the resource_name. For example,
    # the key 'nvidia.com/gpu' contains the key 'gpu'.
    matching_keys = [key for key in resources if resource_name in key.lower()]
    if len(matching_keys) == 0:
        return float("inf")
    if len(matching_keys) > 1:
        # Should have only one match -- mostly relevant for gpu.
        raise ValueError(f"Multiple {resource_name} types not supported.")
    # E.g. 'nvidia.com/gpu' or 'cpu'.
    resource_key = matching_keys.pop()
    resource_quantity = resources[resource_key]
    if resource_name == "memory":
        return _parse_memory_resource(resource_quantity)
    else:
        return _parse_cpu_or_gpu_resource(resource_quantity)


def _parse_cpu_or_gpu_resource(resource):
    resource_str = str(resource)
    if resource_str[-1] == "m":
        # For example, '500m' rounds up to 1.
        return math.ceil(int(resource_str[:-1]) / 1000)
    else:
        return int(resource_str)


def _parse_memory_resource(resource):
    resource_str = str(resource)
    try:
        return int(resource_str)
    except ValueError:
        pass
    memory_size = re.sub(r"([KMGTP]+)", r" \1", resource_str)
    number, unit_index = [item.strip() for item in memory_size.split()]
    unit_index = unit_index[0]
    return float(number) * MEMORY_SIZE_UNITS[unit_index]


def _configure_namespace(provider_config):
    namespace_field = "namespace"
    if namespace_field not in provider_config:
        raise ValueError("Must specify namespace in Kubernetes config.")

    namespace = provider_config[namespace_field]
    field_selector = "metadata.name={}".format(namespace)
    try:
        namespaces = core_api().list_namespace(field_selector=field_selector).items
    except ApiException:
        logger.warning(log_prefix + not_checking_msg(namespace_field, namespace))
        return namespace

    if len(namespaces) > 0:
        assert len(namespaces) == 1
        logger.info(log_prefix + using_existing_msg(namespace_field, namespace))
        return namespace

    logger.info(log_prefix + not_found_msg(namespace_field, namespace))
    namespace_config = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
    core_api().create_namespace(namespace_config)
    logger.info(log_prefix + created_msg(namespace_field, namespace))
    return namespace


def _configure_autoscaler_service_account(namespace, provider_config):
    account_field = "autoscaler_service_account"
    if account_field not in provider_config:
        logger.info(log_prefix + not_provided_msg(account_field))
        return

    account = provider_config[account_field]
    if "namespace" not in account["metadata"]:
        account["metadata"]["namespace"] = namespace
    elif account["metadata"]["namespace"] != namespace:
        raise InvalidNamespaceError(account_field, namespace)

    name = account["metadata"]["name"]
    field_selector = "metadata.name={}".format(name)
    accounts = (
        core_api()
        .list_namespaced_service_account(namespace, field_selector=field_selector)
        .items
    )
    if len(accounts) > 0:
        assert len(accounts) == 1
        logger.info(log_prefix + using_existing_msg(account_field, name))
        return

    logger.info(log_prefix + not_found_msg(account_field, name))
    core_api().create_namespaced_service_account(namespace, account)
    logger.info(log_prefix + created_msg(account_field, name))


def _configure_autoscaler_role(namespace, provider_config):
    role_field = "autoscaler_role"
    if role_field not in provider_config:
        logger.info(log_prefix + not_provided_msg(role_field))
        return

    role = provider_config[role_field]
    if "namespace" not in role["metadata"]:
        role["metadata"]["namespace"] = namespace
    elif role["metadata"]["namespace"] != namespace:
        raise InvalidNamespaceError(role_field, namespace)

    name = role["metadata"]["name"]
    field_selector = "metadata.name={}".format(name)
    accounts = (
        auth_api().list_namespaced_role(namespace, field_selector=field_selector).items
    )
    if len(accounts) > 0:
        assert len(accounts) == 1
        logger.info(log_prefix + using_existing_msg(role_field, name))
        return

    logger.info(log_prefix + not_found_msg(role_field, name))
    auth_api().create_namespaced_role(namespace, role)
    logger.info(log_prefix + created_msg(role_field, name))


def _configure_autoscaler_role_binding(namespace, provider_config):
    binding_field = "autoscaler_role_binding"
    if binding_field not in provider_config:
        logger.info(log_prefix + not_provided_msg(binding_field))
        return

    binding = provider_config[binding_field]
    if "namespace" not in binding["metadata"]:
        binding["metadata"]["namespace"] = namespace
    elif binding["metadata"]["namespace"] != namespace:
        raise InvalidNamespaceError(binding_field, namespace)
    for subject in binding["subjects"]:
        if "namespace" not in subject:
            subject["namespace"] = namespace
        elif subject["namespace"] != namespace:
            raise InvalidNamespaceError(
                binding_field + " subject '{}'".format(subject["name"]), namespace
            )

    name = binding["metadata"]["name"]
    field_selector = "metadata.name={}".format(name)
    accounts = (
        auth_api()
        .list_namespaced_role_binding(namespace, field_selector=field_selector)
        .items
    )
    if len(accounts) > 0:
        assert len(accounts) == 1
        logger.info(log_prefix + using_existing_msg(binding_field, name))
        return

    logger.info(log_prefix + not_found_msg(binding_field, name))
    auth_api().create_namespaced_role_binding(namespace, binding)
    logger.info(log_prefix + created_msg(binding_field, name))


def _configure_services(namespace, provider_config):
    service_field = "services"
    if service_field not in provider_config:
        logger.info(log_prefix + not_provided_msg(service_field))
        return

    services = provider_config[service_field]
    for service in services:
        if "namespace" not in service["metadata"]:
            service["metadata"]["namespace"] = namespace
        elif service["metadata"]["namespace"] != namespace:
            raise InvalidNamespaceError(service_field, namespace)

        name = service["metadata"]["name"]
        field_selector = "metadata.name={}".format(name)
        services = (
            core_api()
            .list_namespaced_service(namespace, field_selector=field_selector)
            .items
        )
        if len(services) > 0:
            assert len(services) == 1
            existing_service = services[0]
            if service == existing_service:
                logger.info(log_prefix + using_existing_msg("service", name))
                return
            else:
                logger.info(log_prefix + updating_existing_msg("service", name))
                core_api().patch_namespaced_service(name, namespace, service)
        else:
            logger.info(log_prefix + not_found_msg("service", name))
            core_api().create_namespaced_service(namespace, service)
            logger.info(log_prefix + created_msg("service", name))
