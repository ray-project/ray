import json
import os
import random
import string
import logging

from ray._private import utils

logger = logging.getLogger(__name__)


def create_cgroup_for_worker(resource_json):
    cgroup_name = "ray-" + "".join(
        random.sample(string.ascii_letters + string.digits, 8))
    allocated_resource = json.loads(resource_json)
    cgroup_nodes = []
    if "CPU" in allocated_resource:
        cpu_resource = allocated_resource["CPU"]
        cpu_shares = 0
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset.cpus
            cpu_ids = []
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpu_shares += val
            utils.create_ray_parent_cgroup("cpuset")
            cgroup_path = utils.create_ray_cgroup("cpuset", cgroup_name)
            utils.set_ray_cgroup_property(cgroup_path, "cpuset.cpus", ",".join(
                str(v) for v in cpu_ids))
            utils.set_ray_cgroup_property(cgroup_path, "cpuset.mems", ",".join(
                str(v) for v in cpu_ids))
            cgroup_nodes.append(cgroup_path)
        else:
            cpu_shares = cpu_resource
        # cpushare
        utils.create_ray_parent_cgroup("cpu")
        cgroup_path = utils.create_ray_cgroup("cpu", cgroup_name)
        utils.set_ray_cgroup_property(cgroup_path, "cpu.shares",
                                      str(int(cpu_shares / 10000 * 1024)))
        cgroup_nodes.append(cgroup_path)
    if "memory" in allocated_resource:
        utils.create_ray_parent_cgroup("memory")
        cgroup_path = utils.create_ray_cgroup("memory", cgroup_name)
        utils.set_ray_cgroup_property(
            cgroup_path, "memory.limit_in_bytes",
            str(int(allocated_resource["memory"] / 10000)))
        cgroup_nodes.append(cgroup_path)
    return cgroup_nodes


def start_worker_in_cgroup(worker_func, worker_func_args, resource_json):
    try:
        cgroup_nodes = create_cgroup_for_worker(resource_json)
    except Exception:
        logger.exception("Failed to create cgroup for worker")
        return

    child_pid = os.fork()
    # todo (chenk008): clean cgroup if fork failed
    if child_pid != 0:
        # the parent process is responsible for cleaning cgroup after
        # the worker process exit.
        os.wait()
        logger.warning(
            "Process {} child process {} exited, clean cgroup:{}".format(
                os.getpid(), child_pid, cgroup_nodes))
        for idx, node in enumerate(cgroup_nodes):
            utils.delete_cgroup_path(node)
    else:
        current_pid = os.getpid()
        for idx, node in enumerate(cgroup_nodes):
            utils.set_ray_cgroup_property(node, "cgroup.procs", current_pid)
        worker_func(worker_func_args)
