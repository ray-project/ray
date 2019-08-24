from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import yaml

from kubernetes.client.rest import ApiException

from ray.autoscaler.kubernetes import auth_api, core_api, log_prefix

logger = logging.getLogger(__name__)

DIR = os.path.dirname(os.path.abspath(__file__))


def bootstrap_kubernetes(config):
    config["provider"]["use_internal_ips"] = True
    config = _configure_namespace(config)
    config = _configure_autoscaler_permissions(config)
    return config


def _configure_namespace(config):
    namespace = _read_default_namespace()
    if ("namespace" in config["provider"] and
            config["provider"]["namespace"] != namespace["metadata"]["name"]):
        logger.info(log_prefix + "using user-provided namespace '{}'".format(
            config["provider"]["namespace"]))
        return config

    config["provider"]["namespace"] = namespace["metadata"]["name"]
    try:
        core_api.create_namespace(namespace)
        logger.info(log_prefix + "created default namespace '{}'".format(
            config["provider"]["namespace"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix + "using existing default namespace '{}'".
                        format(config["provider"]["namespace"]))
        else:
            raise
    return config


def _configure_autoscaler_permissions(config):
    namespace = config["provider"]["namespace"]
    head_pod_service_account = config["head_node"]["spec"].get(
        "serviceAccountName", None)
    if _create_default_service_account(head_pod_service_account, namespace):
        _create_default_role(namespace)
        _create_default_role_binding(namespace)

    return config


def _read_default_namespace():
    with open(os.path.join(DIR, "namespace.yaml")) as f:
        return yaml.safe_load(f)


def _create_default_service_account(head_pod_service_account, namespace):
    with open(os.path.join(DIR, "autoscaler-service-account.yaml")) as f:
        account = yaml.safe_load(f)
        if (head_pod_service_account
                and account["metadata"]["name"] != head_pod_service_account):
            logger.info(log_prefix +
                        "using user-provided service account '{}'".format(
                            account["metadata"]["name"]))
            return False
        account["metadata"]["namespace"] = namespace

    try:
        core_api.create_namespaced_service_account(namespace, account)
        logger.info(log_prefix + "created default service account '{}'".format(
            account["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix +
                        "using existing default service account '{}'".format(
                            account["metadata"]["name"]))
        else:
            raise
    return True


def _create_default_role(namespace):
    with open(os.path.join(DIR, "autoscaler-role.yaml")) as f:
        role = yaml.safe_load(f)
        role["metadata"]["namespace"] = namespace

    try:
        auth_api.create_namespaced_role(namespace, role)
        logger.info(log_prefix + "created default role '{}'".format(
            role["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix + "using existing default role '{}'".format(
                role["metadata"]["name"]))
        else:
            raise


def _create_default_role_binding(namespace):
    with open(os.path.join(DIR, "autoscaler-role-binding.yaml")) as f:
        role_binding = yaml.safe_load(f)
        role_binding["metadata"]["namespace"] = namespace

    try:
        auth_api.create_namespaced_role_binding(namespace, role_binding)
        logger.info(log_prefix + "created default role binding'{}'".format(
            role_binding["metadata"]["name"]))
    except ApiException as e:
        if e.status == 409:
            logger.info(log_prefix +
                        "using existing default role binding '{}'".format(
                            role_binding["metadata"]["name"]))
        else:
            raise
