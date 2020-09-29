"""IMPORTANT: this is an experimental interface and not currently stable."""

from typing import Optional, List, Union
import json
import os
import tempfile

from ray.autoscaler._private import commands


def create_or_update_cluster(cluster_config: Union[dict, str],
                             *,
                             no_restart: bool = False,
                             restart_only: bool = False,
                             no_config_cache: bool = False) -> None:
    """Create or updates an autoscaling Ray cluster from a config json.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        no_restart (bool): Whether to skip restarting Ray services during the
            update. This avoids interrupting running jobs and can be used to
            dynamically adjust autoscaler configuration.
        restart_only (bool): Whether to skip running setup commands and only
            restart Ray. This cannot be used with 'no-restart'.
        no_config_cache (bool): Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.
    """
    return commands.create_or_update_cluster(
        config_file=_as_config_file(cluster_config),
        override_min_workers=None,
        override_max_workers=None,
        no_restart=no_restart,
        restart_only=restart_only,
        yes=True,
        override_cluster_name=None,
        no_config_cache=no_config_cache,
        redirect_command_output=None,
        use_login_shells=True)


def teardown_cluster(cluster_config: Union[dict, str]) -> None:
    """Destroys all nodes of a Ray cluster described by a config json.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
    """
    return commands.teardown_cluster(
        config_file=_as_config_file(cluster_config),
        yes=True,
        workers_only=False,
        override_cluster_name=None,
        keep_min_workers=False)


def run_on_cluster(cluster_config: Union[dict, str],
                   *,
                   cmd: Optional[str] = None,
                   run_env: str = "auto",
                   no_config_cache: bool = False,
                   port_forward: Union[int, List[int]] = None,
                   with_output: bool = False) -> str:
    """Runs a command on the specified cluster.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        cmd (str): the command to run, or None for a no-op command.
        run_env (str): whether to run the command on the host or in a
            container. Select between "auto", "host" and "docker".
        no_config_cache (bool): Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.
        port_forward (int or list[int]): port(s) to forward.
        with_output (bool): Whether to capture command output.

    Returns:
        The output of the command as a string.
    """
    return commands.exec_cluster(
        _as_config_file(cluster_config),
        cmd=cmd,
        run_env=run_env,
        screen=False,
        tmux=False,
        stop=False,
        start=False,
        override_cluster_name=None,
        no_config_cache=no_config_cache,
        port_forward=port_forward,
        with_output=with_output)


def rsync(cluster_config: Union[dict, str],
          *,
          source: str,
          target: str,
          down: bool,
          ip_address: str = None,
          use_internal: bool = False,
          no_config_cache: bool = False):
    """Rsyncs files to or from the cluster.

    Args:
        cluster_config (Union[str, dict]): Either the config dict of the
            cluster, or a path pointing to a file containing the config.
        source (str): rsync source argument.
        target (str): rsync target argument.
        down (bool): whether we're syncing remote -> local.
        ip_address (str): Address of node.
        use_internal (bool): Whether the provided ip_address is
            public or private.
        no_config_cache (bool): Whether to disable the config cache and fully
            resolve all environment settings from the Cloud provider again.

    Raises:
        RuntimeError if the cluster head node is not found.
    """
    return commands.rsync(
        config_file=_as_config_file(cluster_config),
        source=source,
        target=target,
        override_cluster_name=None,
        down=down,
        ip_address=ip_address,
        use_internal=use_internal,
        no_config_cache=no_config_cache,
        all_nodes=False)


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
    return commands.get_head_node_ip(_as_config_file(cluster_config))


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
    return commands.get_worker_node_ips(_as_config_file(cluster_config))


def request_resources(num_cpus=None, bundles=None):
    """Remotely request some CPU or GPU resources from the autoscaler.

    This function is to be called e.g. on a node before submitting a bunch of
    ray.remote calls to ensure that resources rapidly become available.

    This function is EXPERIMENTAL.

    Args:
        num_cpus: int -- the number of CPU cores to request
        bundles: List[dict] -- list of resource dicts (e.g., {"CPU": 1}). This
            only has an effect if you've configured `available_node_types`
            if your cluster config.
    """
    return commands.request_resources(num_cpus, bundles)


def _as_config_file(cluster_config: Union[dict, str]):
    if isinstance(cluster_config, dict):
        tmp = tempfile.NamedTemporaryFile("w", prefix="autoscaler-sdk-tmp-")
        tmp.write(json.dumps(cluster_config))
        tmp.flush()
        cluster_config = tmp.name
    if not os.path.exists(cluster_config):
        raise ValueError("Cluster config not found {}".format(cluster_config))
    return cluster_config
