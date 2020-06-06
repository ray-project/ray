import copy
import hashlib
import json
import os
import tempfile
import time
import logging
import sys
import click
import random

import yaml
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from ray.autoscaler.util import validate_config, hash_runtime_conf, \
    hash_launch_conf, fillout_defaults
from ray.autoscaler.node_provider import get_node_provider, NODE_PROVIDERS
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_NODE_NAME, NODE_TYPE_WORKER, NODE_TYPE_HEAD
from ray.autoscaler.updater import NodeUpdaterThread
from ray.autoscaler.log_timer import LogTimer
from ray.autoscaler.docker import with_docker_exec

logger = logging.getLogger(__name__)


def create_or_update_cluster(config_file, override_min_workers,
                             override_max_workers, no_restart, restart_only,
                             yes, override_cluster_name):
    """Create or updates an autoscaling Ray cluster from a config json."""
    config = yaml.safe_load(open(config_file).read())
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
        logger.info("Using cached config at {}".format(cache_key))
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


def teardown_cluster(config_file, yes, workers_only, override_cluster_name,
                     keep_min_workers):
    """Destroys all nodes of a Ray cluster described by a config json."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = fillout_defaults(config)
    validate_config(config)

    confirm("This will destroy your cluster", yes)

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:

        def remaining_nodes():

            workers = provider.non_terminated_nodes({
                TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER
            })

            if keep_min_workers:
                min_workers = config.get("min_workers", 0)
                logger.info("teardown_cluster: "
                            "Keeping {} nodes...".format(min_workers))
                workers = random.sample(workers, len(workers) - min_workers)

            if workers_only:
                return workers

            head = provider.non_terminated_nodes({
                TAG_RAY_NODE_TYPE: NODE_TYPE_HEAD
            })

            return head + workers

        # Loop here to check that both the head and worker nodes are actually
        #   really gone
        A = remaining_nodes()
        with LogTimer("teardown_cluster: done."):
            while A:
                logger.info("teardown_cluster: "
                            "Shutting down {} nodes...".format(len(A)))
                provider.terminate_nodes(A)
                time.sleep(1)
                A = remaining_nodes()
    finally:
        provider.cleanup()


def kill_node(config_file, yes, hard, override_cluster_name):
    """Kills a random Raylet worker."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)

    confirm("This will kill a node in your cluster", yes)

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        nodes = provider.non_terminated_nodes({
            TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER
        })
        node = random.choice(nodes)
        logger.info("kill_node: Shutdown worker {}".format(node))
        if hard:
            provider.terminate_node(node)
        else:
            updater = NodeUpdaterThread(
                node_id=node,
                provider_config=config["provider"],
                provider=provider,
                auth_config=config["auth"],
                cluster_name=config["cluster_name"],
                file_mounts=config["file_mounts"],
                initialization_commands=[],
                setup_commands=[],
                ray_start_commands=[],
                runtime_hash="")

            _exec(updater, "ray stop", False, False)

        time.sleep(5)

        if config.get("provider", {}).get("use_internal_ips", False) is True:
            node_ip = provider.internal_ip(node)
        else:
            node_ip = provider.external_ip(node)
    finally:
        provider.cleanup()

    return node_ip


def monitor_cluster(cluster_config_file, num_lines, override_cluster_name):
    """Kills a random Raylet worker."""
    cmd = "tail -n {} -f /tmp/ray/session_*/logs/monitor*".format(num_lines)
    exec_cluster(cluster_config_file, cmd, False, False, False, False, False,
                 override_cluster_name, None)


def warn_about_bad_start_command(start_commands):
    ray_start_cmd = list(filter(lambda x: "ray start" in x, start_commands))
    if len(ray_start_cmd) == 0:
        logger.warning(
            "Ray start is not included in the head_start_ray_commands section."
        )
    if not any("autoscaling-config" in x for x in ray_start_cmd):
        logger.warning(
            "Ray start on the head node does not have the flag"
            "--autoscaling-config set. The head node will not launch"
            "workers. Add --autoscaling-config=~/ray_bootstrap_config.yaml"
            "to ray start in the head_start_ray_commands section.")


def get_or_create_head_node(config, config_file, no_restart, restart_only, yes,
                            override_cluster_name):
    """Create the cluster head node, which in turn creates the workers."""
    provider = get_node_provider(config["provider"], config["cluster_name"])
    config_file = os.path.abspath(config_file)
    try:
        head_node_tags = {
            TAG_RAY_NODE_TYPE: NODE_TYPE_HEAD,
        }
        nodes = provider.non_terminated_nodes(head_node_tags)
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
                confirm("Head node config out-of-date. It will be terminated",
                        yes)
                logger.info(
                    "get_or_create_head_node: "
                    "Shutting down outdated head node {}".format(head_node))
                provider.terminate_node(head_node)
            logger.info("get_or_create_head_node: Launching new head node...")
            head_node_tags[TAG_RAY_LAUNCH_CONFIG] = launch_hash
            head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
                config["cluster_name"])
            provider.create_node(config["head_node"], head_node_tags, 1)

        start = time.time()
        head_node = None
        while True:
            if time.time() - start > 5:
                raise RuntimeError("Failed to create head node.")
            nodes = provider.non_terminated_nodes(head_node_tags)
            if len(nodes) == 1:
                head_node = nodes[0]
                break
            time.sleep(1)

        # TODO(ekl) right now we always update the head node even if the hash
        # matches. We could prompt the user for what they want to do here.
        runtime_hash = hash_runtime_conf(config["file_mounts"], config)
        logger.info("get_or_create_head_node: Updating files on head node...")

        # Rewrite the auth config so that the head node can update the workers
        remote_config = copy.deepcopy(config)
        if config["provider"]["type"] != "kubernetes":
            remote_key_path = "~/ray_bootstrap_key.pem"
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
            "~/ray_bootstrap_config.yaml": remote_config_file.name
        })
        if config["provider"]["type"] != "kubernetes":
            config["file_mounts"].update({
                remote_key_path: config["auth"]["ssh_private_key"],
            })

        if restart_only:
            init_commands = []
            ray_start_commands = config["head_start_ray_commands"]
        elif no_restart:
            init_commands = config["head_setup_commands"]
            ray_start_commands = []
        else:
            init_commands = config["head_setup_commands"]
            ray_start_commands = config["head_start_ray_commands"]

        if not no_restart:
            warn_about_bad_start_command(ray_start_commands)

        updater = NodeUpdaterThread(
            node_id=head_node,
            provider_config=config["provider"],
            provider=provider,
            auth_config=config["auth"],
            cluster_name=config["cluster_name"],
            file_mounts=config["file_mounts"],
            initialization_commands=config["initialization_commands"],
            setup_commands=init_commands,
            ray_start_commands=ray_start_commands,
            runtime_hash=runtime_hash,
        )
        updater.start()
        updater.join()

        # Refresh the node cache so we see the external ip if available
        provider.non_terminated_nodes(head_node_tags)

        if config.get("provider", {}).get("use_internal_ips", False) is True:
            head_node_ip = provider.internal_ip(head_node)
        else:
            head_node_ip = provider.external_ip(head_node)

        if updater.exitcode != 0:
            logger.error("get_or_create_head_node: "
                         "Updating {} failed".format(head_node_ip))
            sys.exit(1)
        logger.info(
            "get_or_create_head_node: "
            "Head node up-to-date, IP address is: {}".format(head_node_ip))

        monitor_str = "tail -n 100 -f /tmp/ray/session_*/logs/monitor*"
        use_docker = "docker" in config and bool(
            config["docker"]["container_name"])
        if override_cluster_name:
            modifiers = " --cluster-name={}".format(
                quote(override_cluster_name))
        else:
            modifiers = ""
        print("To monitor auto-scaling activity, you can run:\n\n"
              "  ray exec {} {}{}{}\n".format(
                  config_file, "--docker " if use_docker else "",
                  quote(monitor_str), modifiers))
        print("To open a console on the cluster:\n\n"
              "  ray attach {}{}\n".format(config_file, modifiers))

        print("To get a remote shell to the cluster manually, run:\n\n"
              "  {}\n".format(updater.cmd_runner.remote_shell_command_str()))
    finally:
        provider.cleanup()


def attach_cluster(config_file, start, use_screen, use_tmux,
                   override_cluster_name, new, port_forward):
    """Attaches to a screen for the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        start: whether to start the cluster if it isn't up
        use_screen: whether to use screen as multiplexer
        use_tmux: whether to use tmux as multiplexer
        override_cluster_name: set the name of the cluster
        new: whether to force a new screen
        port_forward (int or list[int]): port(s) to forward
    """

    if use_tmux:
        if new:
            cmd = "tmux new"
        else:
            cmd = "tmux attach || tmux new"
    elif use_screen:
        if new:
            cmd = "screen -L"
        else:
            cmd = "screen -L -xRR"
    else:
        if new:
            raise ValueError(
                "--new only makes sense if passing --screen or --tmux")
        cmd = "$SHELL"

    exec_cluster(config_file, cmd, False, False, False, False, start,
                 override_cluster_name, port_forward)


def exec_cluster(config_file,
                 cmd=None,
                 docker=False,
                 screen=False,
                 tmux=False,
                 stop=False,
                 start=False,
                 override_cluster_name=None,
                 port_forward=None,
                 with_output=False):
    """Runs a command on the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        cmd: command to run
        docker: whether to run command in docker container of config
        screen: whether to run in a screen
        tmux: whether to run in a tmux session
        stop: whether to stop the cluster after command run
        start: whether to start the cluster if it isn't up
        override_cluster_name: set the name of the cluster
        port_forward (int or list[int]): port(s) to forward
    """
    assert not (screen and tmux), "Can specify only one of `screen` or `tmux`."

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)

    head_node = _get_head_node(
        config, config_file, override_cluster_name, create_if_needed=start)

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        updater = NodeUpdaterThread(
            node_id=head_node,
            provider_config=config["provider"],
            provider=provider,
            auth_config=config["auth"],
            cluster_name=config["cluster_name"],
            file_mounts=config["file_mounts"],
            initialization_commands=[],
            setup_commands=[],
            ray_start_commands=[],
            runtime_hash="",
        )

        def wrap_docker(command):
            container_name = config["docker"]["container_name"]
            if not container_name:
                raise ValueError("Docker container not specified in config.")
            return with_docker_exec(
                [command], container_name=container_name)[0]

        if cmd:
            cmd = wrap_docker(cmd) if docker else cmd

            if stop:
                shutdown_cmd = (
                    "ray stop; ray teardown ~/ray_bootstrap_config.yaml "
                    "--yes --workers-only")
                if docker:
                    shutdown_cmd = wrap_docker(shutdown_cmd)
                cmd += ("; {}; sudo shutdown -h now".format(shutdown_cmd))

        result = _exec(
            updater,
            cmd,
            screen,
            tmux,
            port_forward=port_forward,
            with_output=with_output)

        if tmux or screen:
            attach_command_parts = ["ray attach", config_file]
            if override_cluster_name is not None:
                attach_command_parts.append(
                    "--cluster-name={}".format(override_cluster_name))
            if tmux:
                attach_command_parts.append("--tmux")
            elif screen:
                attach_command_parts.append("--screen")

            attach_command = " ".join(attach_command_parts)
            attach_info = "Use `{}` to check on command status.".format(
                attach_command)
            logger.info(attach_info)
        return result
    finally:
        provider.cleanup()


def _exec(updater, cmd, screen, tmux, port_forward=None, with_output=False):
    if cmd:
        if screen:
            cmd = [
                "screen", "-L", "-dm", "bash", "-c",
                quote(cmd + "; exec bash")
            ]
            cmd = " ".join(cmd)
        elif tmux:
            # TODO: Consider providing named session functionality
            cmd = [
                "tmux", "new", "-d", "bash", "-c",
                quote(cmd + "; exec bash")
            ]
            cmd = " ".join(cmd)
    return updater.cmd_runner.run(
        cmd,
        exit_on_fail=True,
        port_forward=port_forward,
        with_output=with_output)


def rsync(config_file,
          source,
          target,
          override_cluster_name,
          down,
          all_nodes=False):
    """Rsyncs files.

    Arguments:
        config_file: path to the cluster yaml
        source: source dir
        target: target dir
        override_cluster_name: set the name of the cluster
        down: whether we're syncing remote -> local
        all_nodes: whether to sync worker nodes in addition to the head node
    """
    assert bool(source) == bool(target), (
        "Must either provide both or neither source and target.")

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        nodes = []
        if all_nodes:
            # technically we re-open the provider for no reason
            # in get_worker_nodes but it's cleaner this way
            # and _get_head_node does this too
            nodes = _get_worker_nodes(config, override_cluster_name)

        nodes += [
            _get_head_node(
                config,
                config_file,
                override_cluster_name,
                create_if_needed=False)
        ]

        for node_id in nodes:
            updater = NodeUpdaterThread(
                node_id=node_id,
                provider_config=config["provider"],
                provider=provider,
                auth_config=config["auth"],
                cluster_name=config["cluster_name"],
                file_mounts=config["file_mounts"],
                initialization_commands=[],
                setup_commands=[],
                ray_start_commands=[],
                runtime_hash="",
            )
            if down:
                rsync = updater.rsync_down
            else:
                rsync = updater.rsync_up

            if source and target:
                rsync(source, target)
            else:
                updater.sync_file_mounts(rsync)

    finally:
        provider.cleanup()


def get_head_node_ip(config_file, override_cluster_name):
    """Returns head node IP for given configuration file if exists."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        head_node = _get_head_node(config, config_file, override_cluster_name)
        if config.get("provider", {}).get("use_internal_ips", False) is True:
            head_node_ip = provider.internal_ip(head_node)
        else:
            head_node_ip = provider.external_ip(head_node)
    finally:
        provider.cleanup()

    return head_node_ip


def get_worker_node_ips(config_file, override_cluster_name):
    """Returns worker node IPs for given configuration file."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        nodes = provider.non_terminated_nodes({
            TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER
        })

        if config.get("provider", {}).get("use_internal_ips", False) is True:
            return [provider.internal_ip(node) for node in nodes]
        else:
            return [provider.external_ip(node) for node in nodes]
    finally:
        provider.cleanup()


def _get_worker_nodes(config, override_cluster_name):
    """Returns worker node ids for given configuration."""
    # todo: technically could be reused in get_worker_node_ips
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        return provider.non_terminated_nodes({
            TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER
        })
    finally:
        provider.cleanup()


def _get_head_node(config,
                   config_file,
                   override_cluster_name,
                   create_if_needed=False):
    provider = get_node_provider(config["provider"], config["cluster_name"])
    try:
        head_node_tags = {
            TAG_RAY_NODE_TYPE: NODE_TYPE_HEAD,
        }
        nodes = provider.non_terminated_nodes(head_node_tags)
    finally:
        provider.cleanup()

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
