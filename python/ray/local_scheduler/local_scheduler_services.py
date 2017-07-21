from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import subprocess
import sys
import time


def random_name():
    return str(random.randint(0, 99999999))


def start_local_scheduler(plasma_store_name,
                          plasma_manager_name=None,
                          worker_path=None,
                          plasma_address=None,
                          node_ip_address="127.0.0.1",
                          redis_address=None,
                          use_valgrind=False,
                          use_profiler=False,
                          stdout_file=None,
                          stderr_file=None,
                          static_resource_list=None,
                          num_workers=0):
    """Start a local scheduler process.

    Args:
        plasma_store_name (str): The name of the plasma store socket to connect
            to.
        plasma_manager_name (str): The name of the plasma manager to connect
            to. This does not need to be provided, but if it is, then the Redis
            address must be provided as well.
        worker_path (str): The path of the worker script to use when the local
            scheduler starts up new workers.
        plasma_address (str): The address of the plasma manager to connect to.
            This is only used by the global scheduler to figure out which
            plasma managers are connected to which local schedulers.
        node_ip_address (str): The address of the node that this local
            scheduler is running on.
        redis_address (str): The address of the Redis instance to connect to.
            If this is not provided, then the local scheduler will not connect
            to Redis.
        use_valgrind (bool): True if the local scheduler should be started
            inside of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the local scheduler should be started
            inside a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        static_resource_list (list): A list of integers specifying the local
            scheduler's resource capacities. The resources should appear in an
            order matching the order defined in task.h.
        num_workers (int): The number of workers that the local scheduler
            should start.

    Return:
        A tuple of the name of the local scheduler socket and the process ID of
            the local scheduler process.
    """
    if (plasma_manager_name is None) != (redis_address is None):
        raise Exception("If one of the plasma_manager_name and the "
                        "redis_address is provided, then both must be "
                        "provided.")
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")
    local_scheduler_executable = os.path.join(os.path.dirname(
        os.path.abspath(__file__)),
        "../core/src/local_scheduler/local_scheduler")
    local_scheduler_name = "/tmp/scheduler{}".format(random_name())
    command = [local_scheduler_executable,
               "-s", local_scheduler_name,
               "-p", plasma_store_name,
               "-h", node_ip_address,
               "-n", str(num_workers)]
    if plasma_manager_name is not None:
        command += ["-m", plasma_manager_name]
    if worker_path is not None:
        assert plasma_store_name is not None
        assert plasma_manager_name is not None
        assert redis_address is not None
        start_worker_command = ("{} {} "
                                "--node-ip-address={} "
                                "--object-store-name={} "
                                "--object-store-manager-name={} "
                                "--local-scheduler-name={} "
                                "--redis-address={}"
                                .format(sys.executable,
                                        worker_path,
                                        node_ip_address,
                                        plasma_store_name,
                                        plasma_manager_name,
                                        local_scheduler_name,
                                        redis_address))
        command += ["-w", start_worker_command]
    if redis_address is not None:
        command += ["-r", redis_address]
    if plasma_address is not None:
        command += ["-a", plasma_address]
    if static_resource_list is not None:
        assert all([isinstance(resource, int) or isinstance(resource, float)
                    for resource in static_resource_list])
        command += ["-c", ",".join([str(resource) for resource
                    in static_resource_list])]

    if use_valgrind:
        pid = subprocess.Popen(["valgrind",
                                "--track-origins=yes",
                                "--leak-check=full",
                                "--show-leak-kinds=all",
                                "--error-exitcode=1"] + command,
                               stdout=stdout_file, stderr=stderr_file)
        time.sleep(1.0)
    elif use_profiler:
        pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command,
                               stdout=stdout_file, stderr=stderr_file)
        time.sleep(1.0)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
    return local_scheduler_name, pid
