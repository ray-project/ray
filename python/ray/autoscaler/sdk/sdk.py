"""IMPORTANT: this is an experimental interface and not currently stable."""

import json
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from ray.autoscaler._private import commands
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.event_system import CreateClusterEvent  # noqa: F401
from ray.autoscaler._private.event_system import global_event_system  # noqa: F401
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def create_or_update_cluster(
    cluster_config: Union[dict, str],
    *,
    no_restart: bool = False,
    restart_only: bool = False,
    no_config_cache: bool = False
) -> Dict[str, Any]:
    """Create or updates an autoscaling Ray cluster from a config json.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        no_restart: Whether to skip restarting Ray services during the
            update. This avoids interrupting running jobs and can be used to
            dynamically adjust autoscaler configuration.
        restart_only: Whether to skip running setup commands and only
            restart Ray. This cannot be used with 'no-restart'.
        no_config_cache: Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.create_or_update_cluster(
            config_file=config_file,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=no_restart,
            restart_only=restart_only,
            yes=True,
            override_cluster_name=None,
            no_config_cache=no_config_cache,
            redirect_command_output=None,
            use_login_shells=True,
        )


@DeveloperAPI
def teardown_cluster(
    cluster_config: Union[dict, str],
    workers_only: bool = False,
    keep_min_workers: bool = False,
) -> None:
    """Destroys all nodes of a Ray cluster described by a config json.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        workers_only: Whether to keep the head node running and only
            teardown worker nodes.
        keep_min_workers: Whether to keep min_workers (as specified
            in the YAML) still running.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.teardown_cluster(
            config_file=config_file,
            yes=True,
            workers_only=workers_only,
            override_cluster_name=None,
            keep_min_workers=keep_min_workers,
        )


@DeveloperAPI
def run_on_cluster(
    cluster_config: Union[dict, str],
    *,
    cmd: Optional[str] = None,
    run_env: str = "auto",
    tmux: bool = False,
    stop: bool = False,
    no_config_cache: bool = False,
    port_forward: Optional[commands.Port_forward] = None,
    with_output: bool = False
) -> Optional[str]:
    """Runs a command on the specified cluster.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        cmd: the command to run, or None for a no-op command.
        run_env: whether to run the command on the host or in a
            container. Select between "auto", "host" and "docker".
        tmux: whether to run in a tmux session
        stop: whether to stop the cluster after command run
        no_config_cache: Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.
        port_forward ( (int,int) or list[(int,int)]): port(s) to forward.
        with_output: Whether to capture command output.

    Returns:
        The output of the command as a string.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.exec_cluster(
            config_file,
            cmd=cmd,
            run_env=run_env,
            screen=False,
            tmux=tmux,
            stop=stop,
            start=False,
            override_cluster_name=None,
            no_config_cache=no_config_cache,
            port_forward=port_forward,
            with_output=with_output,
        )


@DeveloperAPI
def rsync(
    cluster_config: Union[dict, str],
    *,
    source: Optional[str],
    target: Optional[str],
    down: bool,
    ip_address: str = None,
    use_internal_ip: bool = False,
    no_config_cache: bool = False,
    should_bootstrap: bool = True
):
    """Rsyncs files to or from the cluster.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        source: rsync source argument.
        target: rsync target argument.
        down: whether we're syncing remote -> local.
        ip_address: Address of node.
        use_internal_ip: Whether the provided ip_address is
            public or private.
        no_config_cache: Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.
        should_bootstrap: whether to bootstrap cluster config before syncing

    Raises:
        RuntimeError if the cluster head node is not found.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.rsync(
            config_file=config_file,
            source=source,
            target=target,
            override_cluster_name=None,
            down=down,
            ip_address=ip_address,
            use_internal_ip=use_internal_ip,
            no_config_cache=no_config_cache,
            all_nodes=False,
            should_bootstrap=should_bootstrap,
        )


@DeveloperAPI
def get_head_node_ip(cluster_config: Union[dict, str]) -> str:
    """Returns head node IP for given configuration file if exists.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.

    Returns:
        The ip address of the cluster head node.

    Raises:
        RuntimeError if the cluster is not found.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.get_head_node_ip(config_file)


@DeveloperAPI
def get_worker_node_ips(cluster_config: Union[dict, str]) -> List[str]:
    """Returns worker node IPs for given configuration file.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.

    Returns:
        List of worker node ip addresses.

    Raises:
        RuntimeError if the cluster is not found.
    """
    with _as_config_file(cluster_config) as config_file:
        return commands.get_worker_node_ips(config_file)


@DeveloperAPI
def request_resources(
    num_cpus: Optional[int] = None, bundles: Optional[List[dict]] = None
) -> None:
    """Command the autoscaler to scale to accommodate the specified requests.

    The cluster will immediately attempt to scale to accommodate the requested
    resources, bypassing normal upscaling speed constraints. This takes into
    account existing resource usage.

    For example, suppose you call ``request_resources(num_cpus=100)`` and
    there are 45 currently running tasks, each requiring 1 CPU. Then, enough
    nodes will be added so up to 100 tasks can run concurrently. It does
    **not** add enough nodes so that 145 tasks can run.

    This call is only a hint to the autoscaler. The actual resulting cluster
    size may be slightly larger or smaller than expected depending on the
    internal bin packing algorithm and max worker count restrictions.

    Args:
        num_cpus: Scale the cluster to ensure this number of CPUs are
            available. This request is persistent until another call to
            request_resources() is made to override.
        bundles (List[ResourceDict]): Scale the cluster to ensure this set of
            resource shapes can fit. This request is persistent until another
            call to request_resources() is made to override.

    Examples:
        >>> from ray.autoscaler.sdk import request_resources
        >>> # Request 1000 CPUs.
        >>> request_resources(num_cpus=1000) # doctest: +SKIP
        >>> # Request 64 CPUs and also fit a 1-GPU/4-CPU task.
        >>> request_resources( # doctest: +SKIP
        ...     num_cpus=64, bundles=[{"GPU": 1, "CPU": 4}])
        >>> # Same as requesting num_cpus=3.
        >>> request_resources( # doctest: +SKIP
        ...     bundles=[{"CPU": 1}, {"CPU": 1}, {"CPU": 1}])
    """
    return commands.request_resources(num_cpus, bundles)


@DeveloperAPI
def configure_logging(
    log_style: Optional[str] = None,
    color_mode: Optional[str] = None,
    verbosity: Optional[int] = None,
):
    """Configures logging for cluster command calls.

    Args:
        log_style: If 'pretty', outputs with formatting and color.
            If 'record', outputs record-style without formatting.
            'auto' defaults to 'pretty', and disables pretty logging
            if stdin is *not* a TTY. Defaults to "auto".
        color_mode (str):
            Can be "true", "false", or "auto".

            Enables or disables `colorful`.

            If `color_mode` is "auto", is set to `not stdout.isatty()`
        vebosity (int):
            Output verbosity (0, 1, 2, 3).

            Low verbosity will disable `verbose` and `very_verbose` messages.

    """
    cli_logger.configure(
        log_style=log_style, color_mode=color_mode, verbosity=verbosity
    )


@contextmanager
@DeveloperAPI
def _as_config_file(cluster_config: Union[dict, str]) -> Iterator[str]:
    if isinstance(cluster_config, dict):
        tmp = tempfile.NamedTemporaryFile("w", prefix="autoscaler-sdk-tmp-")
        tmp.write(json.dumps(cluster_config))
        tmp.flush()
        cluster_config = tmp.name
    if not os.path.exists(cluster_config):
        raise ValueError("Cluster config not found {}".format(cluster_config))
    yield cluster_config


@DeveloperAPI
def bootstrap_config(
    cluster_config: Dict[str, Any], no_config_cache: bool = False
) -> Dict[str, Any]:
    """Validate and add provider-specific fields to the config. For example,
    IAM/authentication may be added here."""
    return commands._bootstrap_config(cluster_config, no_config_cache)


@DeveloperAPI
def fillout_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    """Fillout default values for a cluster_config based on the provider."""
    from ray.autoscaler._private.util import fillout_defaults

    return fillout_defaults(config)


@DeveloperAPI
def register_callback_handler(
    event_name: str,
    callback: Union[Callable[[Dict], None], List[Callable[[Dict], None]]],
) -> None:
    """Registers a callback handler for autoscaler events.

    Args:
        event_name: Event that callback should be called on. See
            CreateClusterEvent for details on the events available to be
            registered against.
        callback: Callable object that is invoked
            when specified event occurs.
    """
    global_event_system.add_callback_handler(event_name, callback)


@DeveloperAPI
def get_docker_host_mount_location(cluster_name: str) -> str:
    """Return host path that Docker mounts attach to."""
    docker_mount_prefix = "/tmp/ray_tmp_mount/{cluster_name}"
    return docker_mount_prefix.format(cluster_name=cluster_name)
