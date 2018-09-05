from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import hashlib
import json
import os
import tempfile
import time
import sys
import click
import logging

import yaml
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from ray.autoscaler.autoscaler import validate_config, hash_runtime_conf, \
    hash_launch_conf, fillout_defaults
from ray.autoscaler.node_provider import get_node_provider, NODE_PROVIDERS
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_NODE_NAME
from ray.autoscaler.updater import NodeUpdaterProcess

logger = logging.getLogger(__name__)


def create_or_update_cluster(config_file, override_min_workers,
                             override_max_workers, no_restart, restart_only,
                             yes, override_cluster_name):
    """Create or updates an autoscaling Ray cluster from a config json."""
    config = yaml.load(open(config_file).read())
    if override_min_workers is not None:
        config["min_workers"] = override_min_workers
    if override_max_workers is not None:
        config["max_workers"] = override_max_workers
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)
    get_or_create_head_node(config, config_file, no_restart, restart_only, yes,
                            override_cluster_name)


def _bootstrap_config(config):
    config = fillout_defaults(config)

    hasher = hashlib.sha1()
    hasher.update(json.dumps([config], sort_keys=True).encode("utf-8"))
    cache_key = os.path.join(tempfile.gettempdir(),
                             "ray-config-{}".format(hasher.hexdigest()))
    if os.path.exists(cache_key):
        return json.loads(open(cache_key).read())
    validate_config(config)

    importer = NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        raise NotImplementedError("Unsupported provider {}".format(
            config["provider"]))

    bootstrap_config, _ = importer()
    resolved_config = bootstrap_config(config)
    with open(cache_key, "w") as f:
        f.write(json.dumps(resolved_config))
    return resolved_config


def teardown_cluster(config_file, yes, workers_only, override_cluster_name):
    """Destroys all nodes of a Ray cluster described by a config json."""

    config = yaml.load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    validate_config(config)
    config = fillout_defaults(config)

    confirm("This will destroy your cluster", yes)

    provider = get_node_provider(config["provider"], config["cluster_name"])

    if not workers_only:
        for node in provider.nodes({TAG_RAY_NODE_TYPE: "head"}):
            logger.info("Terminating head node {}".format(node))
            provider.terminate_node(node)

    nodes = provider.nodes({TAG_RAY_NODE_TYPE: "worker"})
    while nodes:
        for node in nodes:
            logger.info("Terminating worker {}".format(node))
            provider.terminate_node(node)
        time.sleep(5)
        nodes = provider.nodes({TAG_RAY_NODE_TYPE: "worker"})


def get_or_create_head_node(config, config_file, no_restart, restart_only, yes,
                            override_cluster_name):
    """Create the cluster head node, which in turn creates the workers."""

    provider = get_node_provider(config["provider"], config["cluster_name"])
    head_node_tags = {
        TAG_RAY_NODE_TYPE: "head",
    }
    nodes = provider.nodes(head_node_tags)
    if len(nodes) > 0:
        head_node = nodes[0]
    else:
        head_node = None

    if not head_node:
        confirm("This will create a new cluster", yes)
    elif not no_restart:
        confirm("This will restart cluster services", yes)

    launch_hash = hash_launch_conf(config["head_node"], config["auth"])
    if head_node is None or provider.node_tags(head_node).get(
            TAG_RAY_LAUNCH_CONFIG) != launch_hash:
        if head_node is not None:
            confirm("Head node config out-of-date. It will be terminated", yes)
            logger.info("Terminating outdated head node {}".format(head_node))
            provider.terminate_node(head_node)
        logger.info("Launching new head node...")
        head_node_tags[TAG_RAY_LAUNCH_CONFIG] = launch_hash
        head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
            config["cluster_name"])
        provider.create_node(config["head_node"], head_node_tags, 1)

    nodes = provider.nodes(head_node_tags)
    assert len(nodes) == 1, "Failed to create head node."
    head_node = nodes[0]

    # TODO(ekl) right now we always update the head node even if the hash
    # matches. We could prompt the user for what they want to do in this case.
    runtime_hash = hash_runtime_conf(config["file_mounts"], config)
    logger.info("Updating files on head node...")

    # Rewrite the auth config so that the head node can update the workers
    remote_key_path = "~/ray_bootstrap_key.pem"
    remote_config = copy.deepcopy(config)
    remote_config["auth"]["ssh_private_key"] = remote_key_path

    # Adjust for new file locations
    new_mounts = {}
    for remote_path in config["file_mounts"]:
        new_mounts[remote_path] = remote_path
    remote_config["file_mounts"] = new_mounts
    remote_config["no_restart"] = no_restart

    # Now inject the rewritten config and SSH key into the head node
    remote_config_file = tempfile.NamedTemporaryFile(
        "w", prefix="ray-bootstrap-")
    remote_config_file.write(json.dumps(remote_config))
    remote_config_file.flush()
    config["file_mounts"].update({
        remote_key_path: config["auth"]["ssh_private_key"],
        "~/ray_bootstrap_config.yaml": remote_config_file.name
    })

    if restart_only:
        init_commands = config["head_start_ray_commands"]
    elif no_restart:
        init_commands = (
            config["setup_commands"] + config["head_setup_commands"])
    else:
        init_commands = (
            config["setup_commands"] + config["head_setup_commands"] +
            config["head_start_ray_commands"])

    updater = NodeUpdaterProcess(
        head_node,
        config["provider"],
        config["auth"],
        config["cluster_name"],
        config["file_mounts"],
        init_commands,
        runtime_hash,
        redirect_output=False)
    updater.start()
    updater.join()

    # Refresh the node cache so we see the external ip if available
    provider.nodes(head_node_tags)

    if updater.exitcode != 0:
        logger.error("Updating {} failed".format(
            provider.external_ip(head_node)))
        sys.exit(1)
    logger.info("Head node up-to-date, IP address is: {}".format(
        provider.external_ip(head_node)))

    monitor_str = "tail -n 100 -f /tmp/raylogs/monitor-*"
    for s in init_commands:
        if ("ray start" in s and "docker exec" in s
                and "--autoscaling-config" in s):
            monitor_str = "docker exec {} /bin/sh -c {}".format(
                config["docker"]["container_name"], quote(monitor_str))
    if override_cluster_name:
        modifiers = " --cluster-name={}".format(quote(override_cluster_name))
    else:
        modifiers = ""
    print("To monitor auto-scaling activity, you can run:\n\n"
          "  ray exec {} {}{}\n".format(config_file, quote(monitor_str),
                                        modifiers))
    print("To open a console on the cluster:\n\n"
          "  ray attach {}{}\n".format(config_file, modifiers))
    print("To ssh manually to the cluster, run:\n\n"
          "  ssh -i {} {}@{}\n".format(config["auth"]["ssh_private_key"],
                                       config["auth"]["ssh_user"],
                                       provider.external_ip(head_node)))


def attach_cluster(config_file, start, override_cluster_name):
    """Attaches to a screen for the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        start: whether to start the cluster if it isn't up
        override_cluster_name: set the name of the cluster
    """

    exec_cluster(config_file, "screen -L -xRR", False, False, start,
                 override_cluster_name, None)


def exec_cluster(config_file, cmd, screen, stop, start, override_cluster_name,
                 port_forward):
    """Runs a command on the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        cmd: command to run
        screen: whether to run in a screen
        stop: whether to stop the cluster after command run
        start: whether to start the cluster if it isn't up
        override_cluster_name: set the name of the cluster
        port_forward: port to forward
    """

    config = yaml.load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)
    head_node = _get_head_node(
        config, config_file, override_cluster_name, create_if_needed=start)
    updater = NodeUpdaterProcess(
        head_node,
        config["provider"],
        config["auth"],
        config["cluster_name"],
        config["file_mounts"], [],
        "",
        redirect_output=False)
    if stop:
        cmd += ("; ray stop; ray teardown ~/ray_bootstrap_config.yaml --yes "
                "--workers-only; sudo shutdown -h now")
    _exec(updater, cmd, screen, expect_error=stop, port_forward=port_forward)


def _exec(updater, cmd, screen, expect_error=False, port_forward=None):
    if cmd:
        if screen:
            cmd = [
                "screen", "-L", "-dm", "bash", "-c",
                quote(cmd + "; exec bash")
            ]
            cmd = " ".join(cmd)
        updater.ssh_cmd(
            cmd,
            verbose=False,
            allocate_tty=True,
            expect_error=expect_error,
            port_forward=port_forward)


def rsync(config_file, source, target, override_cluster_name, down):
    """Rsyncs files.

    Arguments:
        config_file: path to the cluster yaml
        source: source dir
        target: target dir
        override_cluster_name: set the name of the cluster
        down: whether we're syncing remote -> local
    """

    config = yaml.load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)
    head_node = _get_head_node(
        config, config_file, override_cluster_name, create_if_needed=False)
    updater = NodeUpdaterProcess(
        head_node,
        config["provider"],
        config["auth"],
        config["cluster_name"],
        config["file_mounts"], [],
        "",
        redirect_output=False)
    if down:
        rsync = updater.rsync_down
    else:
        rsync = updater.rsync_up
    rsync(source, target, check_error=False)


def get_head_node_ip(config_file, override_cluster_name):
    """Returns head node IP for given configuration file if exists."""

    config = yaml.load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    provider = get_node_provider(config["provider"], config["cluster_name"])
    head_node = _get_head_node(config, config_file, override_cluster_name)
    return provider.external_ip(head_node)


def _get_head_node(config,
                   config_file,
                   override_cluster_name,
                   create_if_needed=False):
    provider = get_node_provider(config["provider"], config["cluster_name"])
    head_node_tags = {
        TAG_RAY_NODE_TYPE: "head",
    }
    nodes = provider.nodes(head_node_tags)
    if len(nodes) > 0:
        head_node = nodes[0]
        return head_node
    elif create_if_needed:
        get_or_create_head_node(
            config,
            config_file,
            restart_only=False,
            no_restart=False,
            yes=True,
            override_cluster_name=override_cluster_name)
        return _get_head_node(
            config, config_file, override_cluster_name, create_if_needed=False)
    else:
        raise RuntimeError("Head node of cluster ({}) not found!".format(
            config["cluster_name"]))


def confirm(msg, yes):
    return None if yes else click.confirm(msg, abort=True)
