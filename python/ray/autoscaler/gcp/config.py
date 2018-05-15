from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import time

from googleapiclient import discovery, errors

from ray.ray_constants import BOTO_MAX_RETRIES

crm = discovery.build('cloudresourcemanager', 'v1')
iam = discovery.build('iam', 'v1')
compute = discovery.build('compute', 'v1')


# https://cloud.google.com/docs/compare/aws/

VERSION = 'v1'

RAY = "ray-autoscaler"
DEFAULT_RAY_INSTANCE_PROFILE = RAY + VERSION
DEFAULT_RAY_IAM_ROLE = RAY + VERSION
FIREWALL_NAME_TEMPLATE = RAY + "-{}"

DEFAULT_PROJECT_ID = 'ray-autoscaler-' + VERSION

MAX_POLLS = 12
POLL_INTERVAL = 5


def wait_for_operation(operation):
    print("Waiting for operation {} to finish...".format(operation))

    for _ in range(MAX_POLLS):
        result = crm.operations().get(name=operation['name']).execute()
        if 'error' in result:
            raise Exception(result['error'])

        if 'done' in result and result['done']:
            print("Done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


def key_pair_name(i, region):
    """Returns the ith default (gcp_key_pair_name, key_pair_path)."""
    if i == 0:
        return ("{}_gcp_{}".format(RAY, region),
                os.path.expanduser("~/.ssh/{}_gcp_{}.pem".format(RAY, region)))
    return ("{}_gcp_{}_{}".format(RAY, i, region),
            os.path.expanduser("~/.ssh/{}_gcp_{}_{}.pem".format(RAY, i, region)))


def bootstrap_gcp(config):
    config = _configure_project(config)
    config = _configure_iam_role(config)
    config = _configure_key_pair(config)
    config = _configure_subnet(config)
    config = _configure_firewall_rules(config)

    return config


def _configure_project(config):
    """Setup a gcp project

    Google Compute Platform organizes all the resources, such as storage
    buckets, users, and instances under projects. This is different from
    aws ec2 where everything is global.
    """
    if 'project_id' not in config['provider']:
        print("config.provider.project_id not defined."
              " Using default project_id: {}".format(DEFAULT_PROJECT_ID))

    project_id = config['provider'].get('project_id', DEFAULT_PROJECT_ID)
    project = _get_project(project_id)

    if project is None:
        #  Project not found, try creating it
        result = _create_project(project_id)
        project = _get_project(project_id)

    assert project is not None, "Failed to create project"
    assert project['lifecycleState'] == 'ACTIVE', (
        "Project status needs to be ACTIVE, got {}"
        .format(project['lifecycleState']))

    config['provider']['project_id'] = project['projectId']

    return config


def _configure_iam_role(config):
    """Setup an IAM role that allows instances control compute/storage

    Specifically, the head node needs to have an IAM role that allows it to
    create further gce instances.
    """

    raise NotImplementedError('_configure_iam_role')
    return config


def _configure_key_pair(config):
    """Configure SSH access, using an existing key pair if possible."""

    if "ssh_private_key" in config["auth"]:
        assert "KeyName" in config["head_node"]
        assert "KeyName" in config["worker_nodes"]
        return config

    # Try a few times to get or create a good key pair.
    for i in range(10):
        key_name, key_path = key_pair(i, config["provider"]["region"])
        key = _get_key(key_name, config)

        # Found a good key.
        if key and os.path.exists(key_path):
            break

        # We can safely create a new key.
        if not key and not os.path.exists(key_path):
            print("Creating new key pair {}".format(key_name))
            raise NotImplementedError('create key pair')
            with open(key_path, "w") as f:
                f.write(key.key_material)
            os.chmod(key_path, 0o600)
            break

    assert key, "GCP keypair {} not found for {}".format(key_name, key_path)
    assert os.path.exists(key_path), \
        "Private key file {} not found for {}".format(key_path, key_name)

    print("KeyName not specified for nodes, using {}".format(key_name))

    config["auth"]["ssh_private_key"] = key_path
    config["head_node"]["KeyName"] = key_name
    config["worker_nodes"]["KeyName"] = key_name

    return config


def _configure_subnet(config):
    """Pick a reasonable subnet if not specified by the config."""

    raise NotImplementedError('_configure_subnet')
    return config


def _configure_firewall_rules(config):
    """Configure firewall rules to allow needed traffic to/from the nodes

    Nodes should allow traffic within the worker group, and also SSH access
    from outside.
    """

    raise NotImplementedError('_configure_firewall_rules')
    return config


def _get_subnet(config, subnet_id):
    raise NotImplementedError('_get_subnet')


def _get_role(role_name, config):
    """Return a gcp iam role"""
    raise NotImplementedError('_get_role')


def _get_key(key_name, config):
    """Return a gcp ssh key that matches key_name"""
    raise NotImplementedError('_get_key')


def _get_project(project_id):
    try:
        project = crm.projects().get(projectId=project_id).execute()
    except errors.HttpError as e:
        if e.resp.status != 403: raise
        project = None

    return project


def _create_project(project_id):
    operation = crm.projects().create(
        body={
            'projectId': project_id,
            'name': project_id
        }
    ).execute()

    result = wait_for_operation(operation)

    return result
