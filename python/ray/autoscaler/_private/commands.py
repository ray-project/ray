import copy
import hashlib
import json
import logging
import os
import random
import sys
import subprocess
import tempfile
import time
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, Union

import click
import redis
import yaml
try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

import ray
from ray.experimental.internal_kv import _internal_kv_put
import ray._private.services as services
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler._private.constants import \
    AUTOSCALER_RESOURCE_REQUEST_CHANNEL
from ray.autoscaler._private.util import validate_config, hash_runtime_conf, \
    hash_launch_conf, prepare_config
from ray.autoscaler._private.providers import _get_node_provider, \
    _NODE_PROVIDERS, _PROVIDER_PRETTY_NAMES
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, TAG_RAY_LAUNCH_CONFIG, \
    TAG_RAY_NODE_NAME, NODE_KIND_WORKER, NODE_KIND_HEAD, TAG_RAY_USER_NODE_TYPE
from ray.autoscaler._private.cli_logger import cli_logger, cf
from ray.autoscaler._private.updater import NodeUpdaterThread
from ray.autoscaler._private.command_runner import set_using_login_shells, \
                                          set_rsync_silent
from ray.autoscaler._private.event_system import (CreateClusterEvent,
                                                  global_event_system)
from ray.autoscaler._private.log_timer import LogTimer
from ray.worker import global_worker  # type: ignore
from ray.util.debug import log_once

import ray.autoscaler._private.subprocess_output_util as cmd_output_util
from ray.autoscaler._private.load_metrics import LoadMetricsSummary
from ray.autoscaler._private.autoscaler import AutoscalerSummary
from ray.autoscaler._private.util import format_info_string, \
    format_info_string_no_node_types

logger = logging.getLogger(__name__)

redis_client = None

RUN_ENV_TYPES = ["auto", "host", "docker"]

POLL_INTERVAL = 5

Port_forward = Union[Tuple[int, int], List[Tuple[int, int]]]


def _redis() -> redis.StrictRedis:
    global redis_client
    if redis_client is None:
        redis_client = services.create_redis_client(
            global_worker.node.redis_address,
            password=global_worker.node.redis_password)
    return redis_client


def try_logging_config(config: Dict[str, Any]) -> None:
    if config["provider"]["type"] == "aws":
        from ray.autoscaler._private.aws.config import log_to_cli
        log_to_cli(config)


def try_get_log_state(provider_config: Dict[str, Any]) -> Optional[dict]:
    if provider_config["type"] == "aws":
        from ray.autoscaler._private.aws.config import get_log_state
        return get_log_state()
    return None


def try_reload_log_state(provider_config: Dict[str, Any],
                         log_state: dict) -> None:
    if not log_state:
        return
    if provider_config["type"] == "aws":
        from ray.autoscaler._private.aws.config import reload_log_state
        return reload_log_state(log_state)


def debug_status(status, error) -> str:
    """Return a debug string for the autoscaler."""
    if not status:
        status = "No cluster status."
    else:
        status = status.decode("utf-8")
        as_dict = json.loads(status)
        lm_summary = LoadMetricsSummary(**as_dict["load_metrics_report"])
        if "autoscaler_report" in as_dict:
            autoscaler_summary = AutoscalerSummary(
                **as_dict["autoscaler_report"])
            status = format_info_string(lm_summary, autoscaler_summary)
        else:
            status = format_info_string_no_node_types(lm_summary)
    if error:
        status += "\n"
        status += error.decode("utf-8")
    return status


def request_resources(num_cpus: Optional[int] = None,
                      bundles: Optional[List[dict]] = None) -> None:
    """Remotely request some CPU or GPU resources from the autoscaler.

    This function is to be called e.g. on a node before submitting a bunch of
    ray.remote calls to ensure that resources rapidly become available.

    Args:
        num_cpus (int): Scale the cluster to ensure this number of CPUs are
            available. This request is persistent until another call to
            request_resources() is made.
        bundles (List[ResourceDict]): Scale the cluster to ensure this set of
            resource shapes can fit. This request is persistent until another
            call to request_resources() is made.
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray is not initialized yet")
    to_request = []
    if num_cpus:
        to_request += [{"CPU": 1}] * num_cpus
    if bundles:
        to_request += bundles
    _internal_kv_put(
        AUTOSCALER_RESOURCE_REQUEST_CHANNEL,
        json.dumps(to_request),
        overwrite=True)


def create_or_update_cluster(config_file: str,
                             override_min_workers: Optional[int],
                             override_max_workers: Optional[int],
                             no_restart: bool,
                             restart_only: bool,
                             yes: bool,
                             override_cluster_name: Optional[str] = None,
                             no_config_cache: bool = False,
                             redirect_command_output: Optional[bool] = False,
                             use_login_shells: bool = True) -> Dict[str, Any]:
    """Create or updates an autoscaling Ray cluster from a config json."""
    set_using_login_shells(use_login_shells)
    if not use_login_shells:
        cmd_output_util.set_allow_interactive(False)
    if redirect_command_output is None:
        # Do not redirect by default.
        cmd_output_util.set_output_redirected(False)
    else:
        cmd_output_util.set_output_redirected(redirect_command_output)

    def handle_yaml_error(e):
        cli_logger.error("Cluster config invalid")
        cli_logger.newline()
        cli_logger.error("Failed to load YAML file " + cf.bold("{}"),
                         config_file)
        cli_logger.newline()
        with cli_logger.verbatim_error_ctx("PyYAML error:"):
            cli_logger.error(e)
        cli_logger.abort()

    try:
        config = yaml.safe_load(open(config_file).read())
    except FileNotFoundError:
        cli_logger.abort(
            "Provided cluster configuration file ({}) does not exist",
            cf.bold(config_file))
        raise
    except yaml.parser.ParserError as e:
        handle_yaml_error(e)
        raise
    except yaml.scanner.ScannerError as e:
        handle_yaml_error(e)
        raise
    global_event_system.execute_callback(CreateClusterEvent.up_started,
                                         {"cluster_config": config})

    # todo: validate file_mounts, ssh keys, etc.

    importer = _NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        cli_logger.abort(
            "Unknown provider type " + cf.bold("{}") + "\n"
            "Available providers are: {}", config["provider"]["type"],
            cli_logger.render_list([
                k for k in _NODE_PROVIDERS.keys()
                if _NODE_PROVIDERS[k] is not None
            ]))
        raise NotImplementedError("Unsupported provider {}".format(
            config["provider"]))

    printed_overrides = False

    def handle_cli_override(key, override):
        if override is not None:
            if key in config:
                nonlocal printed_overrides
                printed_overrides = True
                cli_logger.warning(
                    "`{}` override provided on the command line.\n"
                    "  Using " + cf.bold("{}") + cf.dimmed(
                        " [configuration file has " + cf.bold("{}") + "]"),
                    key, override, config[key])
            config[key] = override

    handle_cli_override("min_workers", override_min_workers)
    handle_cli_override("max_workers", override_max_workers)
    handle_cli_override("cluster_name", override_cluster_name)

    if printed_overrides:
        cli_logger.newline()

    cli_logger.labeled_value("Cluster", config["cluster_name"])

    cli_logger.newline()
    config = _bootstrap_config(config, no_config_cache=no_config_cache)

    try_logging_config(config)
    get_or_create_head_node(config, config_file, no_restart, restart_only, yes,
                            override_cluster_name)
    return config


CONFIG_CACHE_VERSION = 1


def _bootstrap_config(config: Dict[str, Any],
                      no_config_cache: bool = False) -> Dict[str, Any]:
    config = prepare_config(config)

    hasher = hashlib.sha1()
    hasher.update(json.dumps([config], sort_keys=True).encode("utf-8"))
    cache_key = os.path.join(tempfile.gettempdir(),
                             "ray-config-{}".format(hasher.hexdigest()))

    if os.path.exists(cache_key) and not no_config_cache:
        config_cache = json.loads(open(cache_key).read())
        if config_cache.get("_version", -1) == CONFIG_CACHE_VERSION:
            # todo: is it fine to re-resolve? afaik it should be.
            # we can have migrations otherwise or something
            # but this seems overcomplicated given that resolving is
            # relatively cheap
            try_reload_log_state(config_cache["config"]["provider"],
                                 config_cache.get("provider_log_info"))

            if log_once("_printed_cached_config_warning"):
                cli_logger.verbose_warning(
                    "Loaded cached provider configuration "
                    "from " + cf.bold("{}"), cache_key)
                if cli_logger.verbosity == 0:
                    cli_logger.warning("Loaded cached provider configuration")
                cli_logger.warning(
                    "If you experience issues with "
                    "the cloud provider, try re-running "
                    "the command with {}.", cf.bold("--no-config-cache"))

            return config_cache["config"]
        else:
            cli_logger.warning(
                "Found cached cluster config "
                "but the version " + cf.bold("{}") + " "
                "(expected " + cf.bold("{}") + ") does not match.\n"
                "This is normal if cluster launcher was updated.\n"
                "Config will be re-resolved.",
                config_cache.get("_version", "none"), CONFIG_CACHE_VERSION)

    importer = _NODE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        raise NotImplementedError("Unsupported provider {}".format(
            config["provider"]))

    provider_cls = importer(config["provider"])

    cli_logger.print("Checking {} environment settings",
                     _PROVIDER_PRETTY_NAMES.get(config["provider"]["type"]))
    try:
        config = provider_cls.fillout_available_node_types_resources(config)
    except Exception as exc:
        if cli_logger.verbosity > 2:
            logger.exception("Failed to autodetect node resources.")
        else:
            cli_logger.warning(
                f"Failed to autodetect node resources: {str(exc)}. "
                "You can see full stack trace with higher verbosity.")

    # NOTE: if `resources` field is missing, validate_config for providers
    # other than AWS and Kubernetes will fail (the schema error will ask the
    # user to manually fill the resources) as we currently support autofilling
    # resources for AWS and Kubernetes only.
    validate_config(config)
    resolved_config = provider_cls.bootstrap_config(config)

    if not no_config_cache:
        with open(cache_key, "w") as f:
            config_cache = {
                "_version": CONFIG_CACHE_VERSION,
                "provider_log_info": try_get_log_state(config["provider"]),
                "config": resolved_config
            }
            f.write(json.dumps(config_cache))
    return resolved_config


def teardown_cluster(config_file: str, yes: bool, workers_only: bool,
                     override_cluster_name: Optional[str],
                     keep_min_workers: bool) -> None:
    """Destroys all nodes of a Ray cluster described by a config json."""
    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    config = _bootstrap_config(config)

    cli_logger.confirm(yes, "Destroying cluster.", _abort=True)

    if not workers_only:
        try:
            exec_cluster(
                config_file,
                cmd="ray stop",
                run_env="auto",
                screen=False,
                tmux=False,
                stop=False,
                start=False,
                override_cluster_name=override_cluster_name,
                port_forward=None,
                with_output=False)
        except Exception as e:
            # todo: add better exception info
            cli_logger.verbose_error("{}", str(e))
            cli_logger.warning(
                "Exception occurred when stopping the cluster Ray runtime "
                "(use -v to dump teardown exceptions).")
            cli_logger.warning(
                "Ignoring the exception and "
                "attempting to shut down the cluster nodes anyway.")

    provider = _get_node_provider(config["provider"], config["cluster_name"])

    def remaining_nodes():
        workers = provider.non_terminated_nodes({
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER
        })

        if keep_min_workers:
            min_workers = config.get("min_workers", 0)
            cli_logger.print(
                "{} random worker nodes will not be shut down. " +
                cf.dimmed("(due to {})"), cf.bold(min_workers),
                cf.bold("--keep-min-workers"))

            workers = random.sample(workers, len(workers) - min_workers)

        # todo: it's weird to kill the head node but not all workers
        if workers_only:
            cli_logger.print(
                "The head node will not be shut down. " +
                cf.dimmed("(due to {})"), cf.bold("--workers-only"))

            return workers

        head = provider.non_terminated_nodes({
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD
        })

        return head + workers

    def run_docker_stop(node, container_name):
        try:
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
                runtime_hash="",
                file_mounts_contents_hash="",
                is_head_node=False,
                docker_config=config.get("docker"))
            _exec(updater, cmd=f"docker stop {container_name}", run_env="host")
        except Exception:
            cli_logger.warning(f"Docker stop failed on {node}")

    # Loop here to check that both the head and worker nodes are actually
    #   really gone
    A = remaining_nodes()

    container_name = config.get("docker", {}).get("container_name")
    if container_name:
        for node in A:
            run_docker_stop(node, container_name)

    with LogTimer("teardown_cluster: done."):
        while A:
            provider.terminate_nodes(A)

            cli_logger.print(
                "Requested {} nodes to shut down.",
                cf.bold(len(A)),
                _tags=dict(interval="1s"))

            time.sleep(POLL_INTERVAL)  # todo: interval should be a variable
            A = remaining_nodes()
            cli_logger.print("{} nodes remaining after {} second(s).",
                             cf.bold(len(A)), POLL_INTERVAL)
        cli_logger.success("No nodes remaining.")


def kill_node(config_file: str, yes: bool, hard: bool,
              override_cluster_name: Optional[str]) -> Optional[str]:
    """Kills a random Raylet worker."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config)

    cli_logger.confirm(yes, "A random node will be killed.")

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    nodes = provider.non_terminated_nodes({
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER
    })
    if not nodes:
        cli_logger.print("No worker nodes detected.")
        return None
    node = random.choice(nodes)
    cli_logger.print("Shutdown " + cf.bold("{}"), node)
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
            runtime_hash="",
            file_mounts_contents_hash="",
            is_head_node=False,
            docker_config=config.get("docker"))

        _exec(updater, "ray stop", False, False)

    time.sleep(POLL_INTERVAL)

    if config.get("provider", {}).get("use_internal_ips", False) is True:
        node_ip = provider.internal_ip(node)
    else:
        node_ip = provider.external_ip(node)

    return node_ip


def monitor_cluster(cluster_config_file: str, num_lines: int,
                    override_cluster_name: Optional[str]) -> None:
    """Tails the autoscaler logs of a Ray cluster."""
    cmd = f"tail -n {num_lines} -f /tmp/ray/session_latest/logs/monitor*"
    exec_cluster(
        cluster_config_file,
        cmd=cmd,
        run_env="auto",
        screen=False,
        tmux=False,
        stop=False,
        start=False,
        override_cluster_name=override_cluster_name,
        port_forward=None)


def warn_about_bad_start_command(start_commands: List[str]) -> None:
    ray_start_cmd = list(filter(lambda x: "ray start" in x, start_commands))
    if len(ray_start_cmd) == 0:
        cli_logger.warning(
            "Ray runtime will not be started because `{}` is not in `{}`.",
            cf.bold("ray start"), cf.bold("head_start_ray_commands"))
    if not any("autoscaling-config" in x for x in ray_start_cmd):
        cli_logger.warning(
            "The head node will not launch any workers because "
            "`{}` does not have `{}` set.\n"
            "Potential fix: add `{}` to the `{}` command under `{}`.",
            cf.bold("ray start"), cf.bold("--autoscaling-config"),
            cf.bold("--autoscaling-config=~/ray_bootstrap_config.yaml"),
            cf.bold("ray start"), cf.bold("head_start_ray_commands"))


def get_or_create_head_node(config: Dict[str, Any],
                            printable_config_file: str,
                            no_restart: bool,
                            restart_only: bool,
                            yes: bool,
                            override_cluster_name: Optional[str],
                            _provider: Optional[NodeProvider] = None,
                            _runner: ModuleType = subprocess) -> None:
    """Create the cluster head node, which in turn creates the workers."""
    global_event_system.execute_callback(
        CreateClusterEvent.cluster_booting_started)
    provider = (_provider or _get_node_provider(config["provider"],
                                                config["cluster_name"]))

    config = copy.deepcopy(config)
    head_node_tags = {
        TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
    }
    nodes = provider.non_terminated_nodes(head_node_tags)
    if len(nodes) > 0:
        head_node = nodes[0]
    else:
        head_node = None

    if not head_node:
        cli_logger.confirm(
            yes,
            "No head node found. "
            "Launching a new cluster.",
            _abort=True)

    if head_node:
        if restart_only:
            cli_logger.confirm(
                yes,
                "Updating cluster configuration and "
                "restarting the cluster Ray runtime. "
                "Setup commands will not be run due to `{}`.\n",
                cf.bold("--restart-only"),
                _abort=True)
        elif no_restart:
            cli_logger.print(
                "Cluster Ray runtime will not be restarted due "
                "to `{}`.", cf.bold("--no-restart"))
            cli_logger.confirm(
                yes,
                "Updating cluster configuration and "
                "running setup commands.",
                _abort=True)
        else:
            cli_logger.print(
                "Updating cluster configuration and running full setup.")
            cli_logger.confirm(
                yes,
                cf.bold("Cluster Ray runtime will be restarted."),
                _abort=True)

    cli_logger.newline()
    # TODO(ekl) this logic is duplicated in node_launcher.py (keep in sync)
    head_node_config = copy.deepcopy(config["head_node"])
    head_node_resources = None
    if "head_node_type" in config:
        head_node_type = config["head_node_type"]
        head_node_tags[TAG_RAY_USER_NODE_TYPE] = head_node_type
        head_config = config["available_node_types"][head_node_type]
        head_node_config.update(head_config["node_config"])

        # Not necessary to keep in sync with node_launcher.py
        # Keep in sync with autoscaler.py _node_resources
        head_node_resources = head_config.get("resources")

    launch_hash = hash_launch_conf(head_node_config, config["auth"])
    if head_node is None or provider.node_tags(head_node).get(
            TAG_RAY_LAUNCH_CONFIG) != launch_hash:
        with cli_logger.group("Acquiring an up-to-date head node"):
            global_event_system.execute_callback(
                CreateClusterEvent.acquiring_new_head_node)
            if head_node is not None:
                cli_logger.print(
                    "Currently running head node is out-of-date with "
                    "cluster configuration")
                cli_logger.print(
                    "hash is {}, expected {}",
                    cf.bold(
                        provider.node_tags(head_node)
                        .get(TAG_RAY_LAUNCH_CONFIG)), cf.bold(launch_hash))
                cli_logger.confirm(yes, "Relaunching it.", _abort=True)

                provider.terminate_node(head_node)
                cli_logger.print("Terminated head node {}", head_node)

            head_node_tags[TAG_RAY_LAUNCH_CONFIG] = launch_hash
            head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
                config["cluster_name"])
            provider.create_node(head_node_config, head_node_tags, 1)
            cli_logger.print("Launched a new head node")

            start = time.time()
            head_node = None
            with cli_logger.group("Fetching the new head node"):
                while True:
                    if time.time() - start > 50:
                        cli_logger.abort(
                            "Head node fetch timed out.")  # todo: msg
                        raise RuntimeError("Failed to create head node.")
                    nodes = provider.non_terminated_nodes(head_node_tags)
                    if len(nodes) == 1:
                        head_node = nodes[0]
                        break
                    time.sleep(POLL_INTERVAL)
            cli_logger.newline()

    global_event_system.execute_callback(CreateClusterEvent.head_node_acquired)

    with cli_logger.group(
            "Setting up head node",
            _numbered=("<>", 1, 1),
            # cf.bold(provider.node_tags(head_node)[TAG_RAY_NODE_NAME]),
            _tags=dict()):  # add id, ARN to tags?

        # TODO(ekl) right now we always update the head node even if the
        # hash matches.
        # We could prompt the user for what they want to do here.
        # No need to pass in cluster_sync_files because we use this
        # hash to set up the head node
        (runtime_hash, file_mounts_contents_hash) = hash_runtime_conf(
            config["file_mounts"], None, config)

        # Rewrite the auth config so that the head
        # node can update the workers
        remote_config = copy.deepcopy(config)

        # drop proxy options if they exist, otherwise
        # head node won't be able to connect to workers
        remote_config["auth"].pop("ssh_proxy_command", None)

        if "ssh_private_key" in config["auth"]:
            remote_key_path = "~/ray_bootstrap_key.pem"
            remote_config["auth"]["ssh_private_key"] = remote_key_path

        # Adjust for new file locations
        new_mounts = {}
        for remote_path in config["file_mounts"]:
            new_mounts[remote_path] = remote_path
        remote_config["file_mounts"] = new_mounts
        remote_config["no_restart"] = no_restart

        remote_config = provider.prepare_for_head_node(remote_config)

        # Now inject the rewritten config and SSH key into the head node
        remote_config_file = tempfile.NamedTemporaryFile(
            "w", prefix="ray-bootstrap-")
        remote_config_file.write(json.dumps(remote_config))
        remote_config_file.flush()
        config["file_mounts"].update({
            "~/ray_bootstrap_config.yaml": remote_config_file.name
        })

        if "ssh_private_key" in config["auth"]:
            config["file_mounts"].update({
                remote_key_path: config["auth"]["ssh_private_key"],
            })
        cli_logger.print("Prepared bootstrap config")

        if restart_only:
            setup_commands = []
            ray_start_commands = config["head_start_ray_commands"]
        elif no_restart:
            setup_commands = config["head_setup_commands"]
            ray_start_commands = []
        else:
            setup_commands = config["head_setup_commands"]
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
            setup_commands=setup_commands,
            ray_start_commands=ray_start_commands,
            process_runner=_runner,
            runtime_hash=runtime_hash,
            file_mounts_contents_hash=file_mounts_contents_hash,
            is_head_node=True,
            node_resources=head_node_resources,
            rsync_options={
                "rsync_exclude": config.get("rsync_exclude"),
                "rsync_filter": config.get("rsync_filter")
            },
            docker_config=config.get("docker"))
        updater.start()
        updater.join()

        # Refresh the node cache so we see the external ip if available
        provider.non_terminated_nodes(head_node_tags)

        if updater.exitcode != 0:
            # todo: this does not follow the mockup and is not good enough
            cli_logger.abort("Failed to setup head node.")
            sys.exit(1)

    global_event_system.execute_callback(
        CreateClusterEvent.cluster_booting_completed, {
            "head_node_id": head_node,
        })

    monitor_str = "tail -n 100 -f /tmp/ray/session_latest/logs/monitor*"
    if override_cluster_name:
        modifiers = " --cluster-name={}".format(quote(override_cluster_name))
    else:
        modifiers = ""

    cli_logger.newline()
    with cli_logger.group("Useful commands"):
        printable_config_file = os.path.abspath(printable_config_file)
        cli_logger.print("Monitor autoscaling with")
        cli_logger.print(
            cf.bold("  ray exec {}{} {}"), printable_config_file, modifiers,
            quote(monitor_str))

        cli_logger.print("Connect to a terminal on the cluster head:")
        cli_logger.print(
            cf.bold("  ray attach {}{}"), printable_config_file, modifiers)

        remote_shell_str = updater.cmd_runner.remote_shell_command_str()
        cli_logger.print("Get a remote shell to the cluster manually:")
        cli_logger.print("  {}", remote_shell_str.strip())


def attach_cluster(config_file: str,
                   start: bool,
                   use_screen: bool,
                   use_tmux: bool,
                   override_cluster_name: Optional[str],
                   no_config_cache: bool = False,
                   new: bool = False,
                   port_forward: Optional[Port_forward] = None) -> None:
    """Attaches to a screen for the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        start: whether to start the cluster if it isn't up
        use_screen: whether to use screen as multiplexer
        use_tmux: whether to use tmux as multiplexer
        override_cluster_name: set the name of the cluster
        new: whether to force a new screen
        port_forward ( (int,int) or list[(int,int)] ): port(s) to forward
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

    exec_cluster(
        config_file,
        cmd=cmd,
        run_env="auto",
        screen=False,
        tmux=False,
        stop=False,
        start=start,
        override_cluster_name=override_cluster_name,
        no_config_cache=no_config_cache,
        port_forward=port_forward,
    )


def exec_cluster(config_file: str,
                 *,
                 cmd: str = None,
                 run_env: str = "auto",
                 screen: bool = False,
                 tmux: bool = False,
                 stop: bool = False,
                 start: bool = False,
                 override_cluster_name: Optional[str] = None,
                 no_config_cache: bool = False,
                 port_forward: Optional[Port_forward] = None,
                 with_output: bool = False) -> str:
    """Runs a command on the specified cluster.

    Arguments:
        config_file: path to the cluster yaml
        cmd: command to run
        run_env: whether to run the command on the host or in a container.
            Select between "auto", "host" and "docker"
        screen: whether to run in a screen
        tmux: whether to run in a tmux session
        stop: whether to stop the cluster after command run
        start: whether to start the cluster if it isn't up
        override_cluster_name: set the name of the cluster
        port_forward ( (int, int) or list[(int, int)] ): port(s) to forward
    """
    assert not (screen and tmux), "Can specify only one of `screen` or `tmux`."
    assert run_env in RUN_ENV_TYPES, "--run_env must be in {}".format(
        RUN_ENV_TYPES)
    # TODO(rliaw): We default this to True to maintain backwards-compat.
    # In the future we would want to support disabling login-shells
    # and interactivity.
    cmd_output_util.set_allow_interactive(True)

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config, no_config_cache=no_config_cache)

    head_node = _get_head_node(
        config, config_file, override_cluster_name, create_if_needed=start)

    provider = _get_node_provider(config["provider"], config["cluster_name"])
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
        file_mounts_contents_hash="",
        is_head_node=True,
        rsync_options={
            "rsync_exclude": config.get("rsync_exclude"),
            "rsync_filter": config.get("rsync_filter")
        },
        docker_config=config.get("docker"))
    shutdown_after_run = False
    if cmd and stop:
        cmd += "; ".join([
            "ray stop",
            "ray teardown ~/ray_bootstrap_config.yaml --yes --workers-only"
        ])
        shutdown_after_run = True

    result = _exec(
        updater,
        cmd,
        screen,
        tmux,
        port_forward=port_forward,
        with_output=with_output,
        run_env=run_env,
        shutdown_after_run=shutdown_after_run)
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
        cli_logger.print("Run `{}` to check command status.",
                         cf.bold(attach_command))
    return result


def _exec(updater: NodeUpdaterThread,
          cmd: Optional[str] = None,
          screen: bool = False,
          tmux: bool = False,
          port_forward: Optional[Port_forward] = None,
          with_output: bool = False,
          run_env: str = "auto",
          shutdown_after_run: bool = False) -> str:
    if cmd:
        if screen:
            wrapped_cmd = [
                "screen", "-L", "-dm", "bash", "-c",
                quote(cmd + "; exec bash")
            ]
            cmd = " ".join(wrapped_cmd)
        elif tmux:
            # TODO: Consider providing named session functionality
            wrapped_cmd = [
                "tmux", "new", "-d", "bash", "-c",
                quote(cmd + "; exec bash")
            ]
            cmd = " ".join(wrapped_cmd)
    return updater.cmd_runner.run(
        cmd,
        exit_on_fail=True,
        port_forward=port_forward,
        with_output=with_output,
        run_env=run_env,
        shutdown_after_run=shutdown_after_run)


def rsync(config_file: str,
          source: Optional[str],
          target: Optional[str],
          override_cluster_name: Optional[str],
          down: bool,
          ip_address: Optional[str] = None,
          use_internal_ip: bool = False,
          no_config_cache: bool = False,
          all_nodes: bool = False,
          _runner: ModuleType = subprocess) -> None:
    """Rsyncs files.

    Arguments:
        config_file: path to the cluster yaml
        source: source dir
        target: target dir
        override_cluster_name: set the name of the cluster
        down: whether we're syncing remote -> local
        ip_address (str): Address of node. Raise Exception
            if both ip_address and 'all_nodes' are provided.
        use_internal_ip (bool): Whether the provided ip_address is
            public or private.
        all_nodes: whether to sync worker nodes in addition to the head node
    """
    if bool(source) != bool(target):
        cli_logger.abort(
            "Expected either both a source and a target, or neither.")

    assert bool(source) == bool(target), (
        "Must either provide both or neither source and target.")

    if ip_address and all_nodes:
        cli_logger.abort("Cannot provide both ip_address and 'all_nodes'.")

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = _bootstrap_config(config, no_config_cache=no_config_cache)

    is_file_mount = False
    if source and target:
        for remote_mount in config.get("file_mounts", {}).keys():
            if (source if down else target).startswith(remote_mount):
                is_file_mount = True
                break

    provider = _get_node_provider(config["provider"], config["cluster_name"])

    def rsync_to_node(node_id, is_head_node):
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
            use_internal_ip=use_internal_ip,
            process_runner=_runner,
            file_mounts_contents_hash="",
            is_head_node=is_head_node,
            rsync_options={
                "rsync_exclude": config.get("rsync_exclude"),
                "rsync_filter": config.get("rsync_filter")
            },
            docker_config=config.get("docker"))
        if down:
            rsync = updater.rsync_down
        else:
            rsync = updater.rsync_up

        if source and target:
            # print rsync progress for single file rsync
            if cli_logger.verbosity > 0:
                cmd_output_util.set_output_redirected(False)
                set_rsync_silent(False)
            rsync(source, target, is_file_mount)
        else:
            updater.sync_file_mounts(rsync)

    nodes = []
    head_node = _get_head_node(
        config, config_file, override_cluster_name, create_if_needed=False)
    if ip_address:
        nodes = [
            provider.get_node_id(ip_address, use_internal_ip=use_internal_ip)
        ]
    else:
        nodes = [head_node]
        if all_nodes:
            nodes.extend(_get_worker_nodes(config, override_cluster_name))

    for node_id in nodes:
        rsync_to_node(node_id, is_head_node=(node_id == head_node))


def get_head_node_ip(config_file: str,
                     override_cluster_name: Optional[str] = None) -> str:
    """Returns head node IP for given configuration file if exists."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    head_node = _get_head_node(config, config_file, override_cluster_name)
    if config.get("provider", {}).get("use_internal_ips", False):
        head_node_ip = provider.internal_ip(head_node)
    else:
        head_node_ip = provider.external_ip(head_node)

    return head_node_ip


def get_worker_node_ips(config_file: str,
                        override_cluster_name: Optional[str] = None
                        ) -> List[str]:
    """Returns worker node IPs for given configuration file."""

    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    nodes = provider.non_terminated_nodes({
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER
    })

    if config.get("provider", {}).get("use_internal_ips", False) is True:
        return [provider.internal_ip(node) for node in nodes]
    else:
        return [provider.external_ip(node) for node in nodes]


def _get_worker_nodes(config: Dict[str, Any],
                      override_cluster_name: Optional[str]) -> List[str]:
    """Returns worker node ids for given configuration."""
    # todo: technically could be reused in get_worker_node_ips
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    return provider.non_terminated_nodes({TAG_RAY_NODE_KIND: NODE_KIND_WORKER})


def _get_head_node(config: Dict[str, Any],
                   printable_config_file: str,
                   override_cluster_name: Optional[str],
                   create_if_needed: bool = False) -> str:
    provider = _get_node_provider(config["provider"], config["cluster_name"])
    head_node_tags = {
        TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
    }
    nodes = provider.non_terminated_nodes(head_node_tags)

    if len(nodes) > 0:
        head_node = nodes[0]
        return head_node
    elif create_if_needed:
        get_or_create_head_node(
            config,
            printable_config_file=printable_config_file,
            restart_only=False,
            no_restart=False,
            yes=True,
            override_cluster_name=override_cluster_name)
        return _get_head_node(
            config,
            printable_config_file,
            override_cluster_name,
            create_if_needed=False)
    else:
        raise RuntimeError("Head node of cluster ({}) not found!".format(
            config["cluster_name"]))


def confirm(msg: str, yes: bool) -> Optional[bool]:
    return None if yes else click.confirm(msg, abort=True)
