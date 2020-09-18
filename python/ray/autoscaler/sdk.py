from typing import Any, Dict, Optional

from ray.autoscaler._private import commands


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


def create_or_update_cluster(config_file: str,
                             override_min_workers: Optional[int],
                             override_max_workers: Optional[int],
                             no_restart: bool,
                             restart_only: bool,
                             yes: bool,
                             override_cluster_name: Optional[str],
                             no_config_cache: bool = False,
                             redirect_command_output: bool = False,
                             use_login_shells: bool = True) -> None:
    """Create or updates an autoscaling Ray cluster from a config json.
    
    TODO(ekl) document this.
    """

    return commands.create_or_update_cluster(
        config_file, override_min_workers, override_max_workers, no_restart,
        restart_only, yes, override_cluster_name, no_config_cache,
        redirect_command_output, use_login_shells)


def teardown_cluster(config_file: str, yes: bool, workers_only: bool,
                     override_cluster_name: Optional[str],
                     keep_min_workers: bool) -> None:
    """Destroys all nodes of a Ray cluster described by a config json.

    TODO(ekl) document this.
    """

    return commands.teardown_cluster(config_file, yes, workers_only,
                                     override_cluster_name, keep_min_workers)


def exec_cluster(config_file: str,
                 *,
                 cmd: Any = None,
                 run_env: str = "auto",
                 screen: bool = False,
                 tmux: bool = False,
                 stop: bool = False,
                 start: bool = False,
                 override_cluster_name: Optional[str] = None,
                 no_config_cache: bool = False,
                 port_forward: Any = None,
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
        port_forward (int or list[int]): port(s) to forward
    """
    return commands.exec_cluster(
        config_file,
        cmd=cmd,
        run_env=run_env,
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=start,
        override_cluster_name=override_cluster_name,
        no_config_cache=no_config_cache,
        port_forward=port_forward,
        with_output=with_output)


def rsync(config_file: str,
          source: Optional[str],
          target: Optional[str],
          override_cluster_name: Optional[str],
          down: bool,
          no_config_cache: bool = False,
          all_nodes: bool = False):
    """Rsyncs files.

    Arguments:
        config_file: path to the cluster yaml
        source: source dir
        target: target dir
        override_cluster_name: set the name of the cluster
        down: whether we're syncing remote -> local
        all_nodes: whether to sync worker nodes in addition to the head node
    """

    return commands.rsync(config_file, source, target, override_cluster_name,
                          down, no_config_cache, all_nodes)


def get_head_node_ip(config_file: str,
                     override_cluster_name: Optional[str]) -> str:
    """Returns head node IP for given configuration file if exists."""

    return commands.get_head_node_ip(config_file, override_cluster_name)


def get_worker_node_ips(config_file: str,
                        override_cluster_name: Optional[str]) -> List[str]:
    """Returns worker node IPs for given configuration file."""

    return commands.get_worker_node_ips(config_file, override_cluster_name)
