import errno
import json
import os
import random
import string

cgroup_v1_root = "/sys/fs/cgroup"
ray_parent_cgroup_name = "ray"


# The parent cgroup can help to limit the resource all ray workers can use
def create_ray_parent_cgroup(controller):
    ray_parent_cgroup = os.path.join(cgroup_v1_root, controller, ray_parent_cgroup_name)
    try:
        os.mkdir(ray_parent_cgroup)
        if controller == "cpuset":
            cpuset_root = os.path.join(cgroup_v1_root, controller)
            cpus = get_ray_cgroup_property(cpuset_root, "cpuset.cpus")
            mems = get_ray_cgroup_property(cpuset_root, "cpuset.mems")
            set_ray_cgroup_property(ray_parent_cgroup, "cpuset.cpus", cpus)
            set_ray_cgroup_property(ray_parent_cgroup, "cpuset.mems", mems)
        return ray_parent_cgroup
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def create_ray_cgroup(controller, cgroup_name):
    ray_cgroup = os.path.join(cgroup_v1_root, controller, ray_parent_cgroup_name, cgroup_name)
    os.mkdir(ray_cgroup)
    return ray_cgroup


def set_ray_cgroup_property(cgroup_path, property, data):
    filename = os.path.join(cgroup_path, property)
    with open(filename, "w") as f:
        return f.write(str(data))


def get_ray_cgroup_property(cgroup_path, property):
    filename = os.path.join(cgroup_path, property)
    with open(filename) as f:
        return f.read().strip()


def delete_cgroup_path(cgroup_path):
    if os.path.exists(cgroup_path):
        os.rmdir(cgroup_path)


def create_cgroup_for_worker(resource_json):
    # todo add timestampe?
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
            create_ray_parent_cgroup("cpuset")
            cgroup_path = create_ray_cgroup("cpuset", cgroup_name)
            set_ray_cgroup_property(cgroup_path, "cpuset.cpus", ",".join(str(v) for v in cpu_ids))
            set_ray_cgroup_property(cgroup_path, "cpuset.mems", ",".join(str(v) for v in cpu_ids))
            cgroup_nodes.append(cgroup_path)
        else:
            cpu_shares = cpu_resource
        # cpushare
        create_ray_parent_cgroup("cpu")
        cgroup_path = create_ray_cgroup("cpu", cgroup_name)
        set_ray_cgroup_property(cgroup_path, "cpu.shares", str(int(cpu_shares / 10000 * 1024)))
        cgroup_nodes.append(cgroup_path)
    if "memory" in allocated_resource:
        create_ray_parent_cgroup("memory")
        cgroup_path = create_ray_cgroup("memory", cgroup_name)
        set_ray_cgroup_property(cgroup_path, "memory.limit_in_bytes", str(int(allocated_resource["memory"] / 10000)))
        cgroup_nodes.append(cgroup_path)
    return cgroup_nodes


def start_worker_in_cgroup(worker_func, resource_json):
    child_pid = os.fork()
    cgroup_nodes = create_cgroup_for_worker(resource_json)
    if child_pid != 0:
        # the parent process is responsible for cleaning cgroup after
        # the worker process exit.
        os.wait()
        for idx, node in enumerate(cgroup_nodes):
            delete_cgroup_path(node)
    else:
        current_pid = os.getpid()
        for idx, node in enumerate(cgroup_nodes):
            set_ray_cgroup_property(node, "cgroup.procs", current_pid)
        worker_func()
