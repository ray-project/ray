import json
import os
import random
import string

import cgroupspy.trees
import cgroupspy.controllers


def create_cgroup_for_worker(resource_json):
    t = cgroupspy.trees.Tree()
    # todo add timestampe?
    cgroup_name = "ray-" + "".join(
        random.sample(string.ascii_letters + string.digits, 8))
    allocated_resource = json.loads(resource_json)
    cgroup_nodes = []
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        cpu_shares = 0
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset-cpus
            cpu_ids = []
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpu_shares += val
            cpu_cset = t.get_node_by_path("/cpuset")
            worker_cpuset_cset = cpu_cset.create_cgroup(cgroup_name)
            ctl = cgroupspy.controllers.Controller(worker_cpuset_cset)
            ctl.set_property(b"cpuset.cpus", ",".join(str(v) for v in cpu_ids))
            ctl.set_property(b"cpuset.mems", ",".join(str(v) for v in cpu_ids))
            cgroup_nodes.append(worker_cpuset_cset)
        else:
            cpu_shares = cpu_resource
        # cpushare
        cpu_cset = t.get_node_by_path("/cpu")
        worker_cpu_cset = cpu_cset.create_cgroup(cgroup_name)
        ctl = cgroupspy.controllers.Controller(worker_cpu_cset)
        ctl.set_property(b"cpu.shares", str(int(cpu_shares / 10000 * 1024)))
        cgroup_nodes.append(worker_cpu_cset)
    if "memory" in allocated_resource.keys():
        memory_cset = t.get_node_by_path("/memory")
        worker_memory_cset = memory_cset.create_cgroup(cgroup_name)
        ctl = cgroupspy.controllers.Controller(worker_memory_cset)
        ctl.set_property(b"memory.limit_in_bytes",
                         str(int(allocated_resource["memory"] / 10000)))
        cgroup_nodes.append(worker_memory_cset)
    return cgroup_nodes


def start_worker_in_cgroup(worker_func, resource_json):
    child_pid = os.fork()
    cgroup_nodes = create_cgroup_for_worker(resource_json)
    if child_pid != 0:
        # the parent process is responsible for cleaning cgroup after
        # the worker process exit.
        os.wait()
        for idx, node in enumerate(cgroup_nodes):
            node.parent.delete_cgroup(str(node.name))
    else:
        current_pid = os.getpid()
        for idx, node in enumerate(cgroup_nodes):
            ctl = cgroupspy.controllers.Controller(node)
            ctl.set_property(b"cgroup.procs", current_pid)
        worker_func()
