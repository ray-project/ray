from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import time

from ray.session import RayNodeSession, PROCESS_TYPE_WORKER
from ray.utils import get_node_ip_address

# TODO(suquark): This field is unused. Maybe remove it?
CREDIS_MEMBER_MODULE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "core/src/credis/build/src/libmember.so")

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray configures it by default automatically
# using logging.basicConfig in its entry/init points.
logger = logging.getLogger(__name__)

# The current Ray session
current_session = None

def cleanup():
    """When running in local mode, shutdown the Ray processes.

    This method is used to shutdown processes that were started with
    services.start_ray_head(). It kills all scheduler, object store, and worker
    processes that were started by this services module. Driver processes are
    started and disconnected by worker.py.
    """
    current_session.shutdown()


def check_orphan_processes():
    # If this is not a driver, make sure there are no orphan processes,
    # besides possibly the worker itself.
    if current_session is not None:
        for process_type, processes in current_session.all_processes.items():
            if process_type == PROCESS_TYPE_WORKER:
                assert len(processes) <= 1
            else:
                assert len(processes) == 0


def all_processes_alive(exclude=None):
    """Check if all of the processes are still alive.

    Args:
        exclude: Don't check the processes whose types are in this list.
    """
    return current_session.all_processes_alive(exclude=exclude)


def start_ray_node(ray_params, cleanup=True):
    """Start the Ray processes for a single node.

    This assumes that the Ray processes on some master node have already been
    started.

    Args:
        ray_params (ray.params.RayParams): The RayParams instance. The
            following parameters could be checked: node_ip_address,
            redis_address, object_manager_port, node_manager_port,
            num_workers, num_local_schedulers, object_store_memory,
            redis_password, worker_path, cleanup, redirect_worker_output,
            redirect_output, resources, plasma_directory, huge_pages,
            plasma_store_socket_name, raylet_socket_name, temp_dir,
            _internal_config
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    global current_session
    ray_params.update(include_log_monitor=True)
    session = RayNodeSession(ray_params)
    session.start_ray_processes(cleanup=cleanup)
    current_session = session
    return session


def start_ray_head(ray_params, cleanup=True):
    """Start Ray in local mode.

    Args:
        ray_params (ray.params.RayParams): The RayParams instance. The
            following parameters could be checked: address_info,
            object_manager_port, node_manager_port, node_ip_address,
            redis_port, redis_shard_ports, num_workers, num_local_schedulers,
            object_store_memory, redis_max_memory, worker_path, cleanup,
            redirect_worker_output, redirect_output,
            start_workers_from_local_scheduler, resources, num_redis_shards,
            redis_max_clients, redis_password, include_webui, huge_pages,
            plasma_directory, autoscaling_config, plasma_store_socket_name,
            raylet_socket_name, temp_dir, _internal_config
        cleanup (bool): If cleanup is true, then the processes started here
            will be killed by services.cleanup() when the Python process that
            called this method exits.

    Returns:
        A dictionary of the address information for the processes that were
            started.
    """
    global current_session
    ray_params.update_if_absent(num_redis_shards=1, include_webui=True)
    ray_params.update(include_log_monitor=True)
    session = RayNodeSession(ray_params)
    session.start_ray_processes(cleanup=cleanup)
    current_session = session
    return session


def connect_cluster(ray_params, num_retries=5):
    if ray_params.redis_address is None:
        raise Exception("When connecting to an existing cluster, "
                        "redis_address must be provided.")
    if ray_params.num_workers is not None:
        raise Exception("When connecting to an existing cluster, "
                        "num_workers must not be provided.")
    if ray_params.num_local_schedulers is not None:
        raise Exception("When connecting to an existing cluster, "
                        "num_local_schedulers must not be provided.")
    if ray_params.num_cpus is not None or ray_params.num_gpus is not None:
        raise Exception("When connecting to an existing cluster, num_cpus "
                        "and num_gpus must not be provided.")
    if ray_params.resources is not None:
        raise Exception("When connecting to an existing cluster, "
                        "resources must not be provided.")
    if ray_params.num_redis_shards is not None:
        raise Exception("When connecting to an existing cluster, "
                        "num_redis_shards must not be provided.")
    if ray_params.redis_max_clients is not None:
        raise Exception("When connecting to an existing cluster, "
                        "redis_max_clients must not be provided.")
    if ray_params.object_store_memory is not None:
        raise Exception("When connecting to an existing cluster, "
                        "object_store_memory must not be provided.")
    if ray_params.redis_max_memory is not None:
        raise Exception("When connecting to an existing cluster, "
                        "redis_max_memory must not be provided.")
    if ray_params.plasma_directory is not None:
        raise Exception("When connecting to an existing cluster, "
                        "plasma_directory must not be provided.")
    if ray_params.huge_pages:
        raise Exception("When connecting to an existing cluster, "
                        "huge_pages must not be provided.")
    if ray_params.temp_dir is not None:
        raise Exception("When connecting to an existing cluster, "
                        "temp_dir must not be provided.")
    if ray_params.plasma_store_socket_name is not None:
        raise Exception("When connecting to an existing cluster, "
                        "plasma_store_socket_name must not be provided.")
    if ray_params.raylet_socket_name is not None:
        raise Exception("When connecting to an existing cluster, "
                        "raylet_socket_name must not be provided.")
    if ray_params._internal_config is not None:
        raise Exception("When connecting to an existing cluster, "
                        "_internal_config must not be provided.")

    # Get the node IP address if one is not provided.
    ray_params.update_if_absent(
        node_ip_address=get_node_ip_address(ray_params.redis_address))
    session = RayNodeSession(ray_params)

    counter = 0
    while True:
        try:
            session.connect_cluster()
            break
        except Exception:
            if counter == num_retries:
                raise
            # Some of the information may not be in Redis yet, so wait a little
            # bit.
            logger.warning(
                "Some processes that the driver needs to connect to have "
                "not registered with Redis, so retrying. Have you run "
                "'ray start' on this node?")
            time.sleep(1)
        counter += 1

    global current_session
    current_session = session
    return session
