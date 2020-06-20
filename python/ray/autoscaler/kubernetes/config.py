import logging

from kubernetes import client

from ray.autoscaler.kubernetes import auth_api, core_api, log_prefix

logger = logging.getLogger(__name__)


class InvalidNamespaceError(ValueError):
    def __init__(self, field_name, namespace):
        self.message = ("Namespace of {} config doesn't match provided "
                        "namespace '{}'. Either set it to {} or remove the "
                        "field".format(field_name, namespace, namespace))

    def __str__(self):
        return self.message


def using_existing_msg(resource_type, name):
    return "using existing {} '{}'".format(resource_type, name)


def updating_existing_msg(resource_type, name):
    return "updating existing {} '{}'".format(resource_type, name)


def not_found_msg(resource_type, name):
    return "{} '{}' not found, attempting to create it".format(
        resource_type, name)


def created_msg(resource_type, name):
    return "successfully created {} '{}'".format(resource_type, name)


def not_provided_msg(resource_type):
    return "no {} config provided, must already exist".format(resource_type)


def bootstrap_kubernetes(config):
    if not config["provider"]["use_internal_ips"]:
        return ValueError("Exposing external IP addresses for ray pods isn't "
                          "currently supported. Please set "
                          "'use_internal_ips' to false.")
    namespace = _configure_namespace(config["provider"])
    _configure_autoscaler_service_account(namespace, config["provider"])
    _configure_autoscaler_role(namespace, config["provider"])
    _configure_autoscaler_role_binding(namespace, config["provider"])
    _configure_services(namespace, config["provider"])
    return config


def _configure_namespace(provider_config):
    namespace_field = "namespace"
    if namespace_field not in provider_config:
        raise ValueError("Must specify namespace in Kubernetes config.")

    namespace = provider_config[namespace_field]
    field_selector = "metadata.name={}".format(namespace)
    namespaces = core_api().list_namespace(field_selector=field_selector).items
    if len(namespaces) > 0:
        assert len(namespaces) == 1
        logger.info(log_prefix +
                    using_existing_msg(namespace_field, namespace))
        return namespace

    logger.info(log_prefix + not_found_msg(namespace_field, namespace))
    namespace_config = client.V1Namespace(
        metadata=client.V1ObjectMeta(name=namespace))
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
    accounts = core_api().list_namespaced_service_account(
        namespace, field_selector=field_selector).items
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
    accounts = auth_api().list_namespaced_role(
        namespace, field_selector=field_selector).items
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
                binding_field + " subject '{}'".format(subject["name"]),
                namespace)

    name = binding["metadata"]["name"]
    field_selector = "metadata.name={}".format(name)
    accounts = auth_api().list_namespaced_role_binding(
        namespace, field_selector=field_selector).items
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
        services = core_api().list_namespaced_service(
            namespace, field_selector=field_selector).items
        if len(services) > 0:
            assert len(services) == 1
            existing_service = services[0]
            if service == existing_service:
                logger.info(log_prefix + using_existing_msg("service", name))
                return
            else:
                logger.info(log_prefix +
                            updating_existing_msg("service", name))
                core_api().patch_namespaced_service(name, namespace, service)
        else:
            logger.info(log_prefix + not_found_msg("service", name))
            core_api().create_namespaced_service(namespace, service)
            logger.info(log_prefix + created_msg("service", name))
