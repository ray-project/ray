from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from kubernetes.client.rest import ApiException

from ray.autoscaler.kubernetes import auth_api, core_api, log_prefix

logger = logging.getLogger(__name__)


def bootstrap_kubernetes(config):
    config["provider"]["use_internal_ips"] = True
    namespace = _configure_namespace(config["provider"])
    _configure_autoscaler_service_account(namespace, config["provider"])
    _configure_autoscaler_role(namespace, config["provider"])
    _configure_autoscaler_role_binding(namespace, config["provider"])
    return config


# TODO: check they're equal if exists?
def _configure_namespace(provider_config):
    if "namespace" not in provider_config:
        raise TypeError("Must specify namespace in Kubernetes config.")

    name = provider_config["namespace"]["metadata"]["name"]
    try:
        core_api.create_namespace(provider_config["namespace"])
        logger.info(log_prefix + "created namespace '{}'".format(name))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix +
                        "using existing namespace '{}'".format(name))
        else:
            raise
    return name


def _configure_autoscaler_service_account(namespace, provider_config):
    if "autoscaler_service_account" not in provider_config:
        logger.info(
            log_prefix +
            "no autoscaler service account provided, must already exist.")
        return

    account = provider_config["autoscaler_service_account"]
    try:
        core_api.create_namespaced_service_account(namespace, account)
        logger.info(log_prefix + "created service account '{}'".format(
            account["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix + "using existing service account '{}'".
                        format(account["metadata"]["name"]))
        else:
            raise


def _configure_autoscaler_role(namespace, provider_config):
    if "autoscaler_role" not in provider_config:
        logger.info(log_prefix +
                    "no autoscaler role provided, must already exist.")
        return

    role = provider_config["autoscaler_role"]
    try:
        auth_api.create_namespaced_role(namespace, role)
        logger.info(log_prefix +
                    "created role '{}'".format(role["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix + "using existing service role '{}'".format(
                role["metadata"]["name"]))
        else:
            raise


def _configure_autoscaler_role_binding(namespace, provider_config):
    if "autoscaler_role_binding" not in provider_config:
        logger.info(log_prefix +
                    "no autoscaler role binding provided, must already exist.")
        return

    binding = provider_config["autoscaler_role_binding"]
    try:
        auth_api.create_namespaced_role_binding(namespace, binding)
        logger.info(log_prefix + "created role binding '{}'".format(
            binding["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix +
                        "using existing service role binding '{}'".format(
                            binding["metadata"]["name"]))
        else:
            raise
