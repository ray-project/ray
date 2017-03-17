from __future__ import print_function

import argparse
import re
import sys
import time

from subprocess32 import Popen, PIPE

class DockerRunner(object):

    def __init__(self):
        self.num_workers = None
        self.head_container_id = None
        self.worker_container_ids = []
        self.head_container_ip = None

    def _get_container_id(self, stdoutdata):
        p = re.compile("([0-9a-f]{64})\n")
        m = p.match(stdoutdata)
        if not m:
            return None
        else:
            return m.group(1)

    def _get_container_ip(self, container_id):
        proc = Popen(["docker", "inspect", "--format={{.NetworkSettings.Networks.bridge.IPAddress}}", container_id], stdout=PIPE, stderr=PIPE)
        (stdoutdata, _) = proc.communicate()
        p = re.compile("([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})")
        m = p.match(stdoutdata)
        if not m:
            raise RuntimeError("Container IP not found")
        else:
            return m.group(1)

    def _start_head_node(self, docker_image, mem_size, shm_size, num_workers):
        mem_arg = ["--memory=" + mem_size] if mem_size else []
        shm_arg = ["--shm-size=" + shm_size] if shm_size else []
        proc = Popen(["docker", "run", "-d"] + mem_arg + shm_arg + [docker_image, "/ray/scripts/start_ray.sh", "--head", "--redis-port=6379", "--num-workers={:d}".format(num_workers)], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        print("start_node", {
            "container_id" : container_id,
            "is_head" : True,
            "num_workers" : num_workers,
            "shm_size" : shm_size
            })
        if not container_id:
            raise RuntimeError("Failed to find container id")
        self.head_container_id = container_id
        self.head_container_ip = self._get_container_ip(container_id)
        return container_id

    def _start_worker_node(self, docker_image, mem_size, shm_size, num_workers):
        mem_arg = ["--memory=" + mem_size] if mem_size else []
        shm_arg = ["--shm-size=" + shm_size] if shm_size else []
        proc = Popen(["docker", "run", "-d"] + mem_arg + shm_arg + ["--shm-size=" + shm_size, docker_image, "/ray/scripts/start_ray.sh", "--redis-address={:s}:6379".format(self.head_container_ip), "--num-workers={:d}".format(num_workers)], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        container_id = self._get_container_id(stdoutdata)
        if not container_id:
            raise RuntimeError("Failed to find container id")
        self.worker_container_ids.append(container_id)
        print("start_node", {
            "container_id" : container_id,
            "is_head" : False,
            "num_workers" : num_workers,
            "shm_size" : shm_size
            })

    def start_ray(self, docker_image, mem_size, shm_size, num_workers, num_nodes):
        if num_workers < num_nodes:
            raise RuntimeError("number of workers must exceed number of nodes")
        self.num_workers = num_workers
        total_procs = num_workers + 2
        workers_per_node_a = int(total_procs / num_nodes)
        workers_per_node_b = workers_per_node_a + 1
        n_a = workers_per_node_b * num_nodes - total_procs
        n_b = num_nodes - n_a

        if n_b > 0:
            workers_per_node_h = workers_per_node_b - 2
            n_b = n_b - 1
        else:
            workers_per_node_h = workers_per_node_a - 2
            n_a = n_a - 1

        # launch the head node
        self._start_head_node(docker_image, mem_size, shm_size, workers_per_node_h)
        for _ in range(n_a):
            self._start_worker_node(docker_image, mem_size, shm_size, workers_per_node_a)
        for _ in range(n_b):
            self._start_worker_node(docker_image, mem_size, shm_size, workers_per_node_b)


    def _stop_node(self, container_id):
        proc = Popen(["docker", "kill", container_id], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        stopped_container_id = self._get_container_id(stdoutdata)
        stop_successful = container_id == stopped_container_id

        proc = Popen(["docker", "rm", "-f", container_id], stdout=PIPE)
        (stdoutdata, stderrdata) = proc.communicate()
        removed_container_id = self._get_container_id(stdoutdata)
        remove_successful = container_id == stopped_container_id

        print("stop_node", {
            "container_id" : container_id,
            "is_head" : container_id == self.head_container_id,
            "stop_success" : stop_successful,
            "remove_success": remove_successful
            })

    def stop_ray(self):
        self._stop_node(self.head_container_id)
        for container_id in self.worker_container_ids:
            self._stop_node(container_id)

    def run_test(self, workload_script, waited_time_limit=None):
        benchmark_iteration = 0
        proc = Popen(["docker", "exec",
            self.head_container_id,
            "/bin/bash", "-c",
            "RAY_BENCHMARK_ENVIRONMENT=stress RAY_BENCHMARK_ITERATION={} RAY_REDIS_ADDRESS={}:6379 RAY_NUM_WORKERS={} python {}".format(benchmark_iteration, self.head_container_ip, self.num_workers, workload_script)], stdout=PIPE, stderr=PIPE)

        start_time = time.time()
        done = False
        while not done:
            try:
                (stdoutdata, stderrdata) = proc.communicate(timeout=min(10, waited_time_limit))
                done = True
            except(subprocess32.TimeoutExpired):
                waited_time = time.time() - start_time
                if waited_time_limit and waited_time > waited_time_limit:
                    # self.logger.log("killed", {
                    #     "pid" : proc.pid,
                    #     "waited_time" : waited_time,
                    #     "waited_time_limit" : waited_time_limit
                    #     })
                    proc.kill()
                    return {
                        "success" : False,
                        "return_code" : None,
                        "stats" : {}
                        }
                else:
                    pass
                    # self.logger.log("waiting", {
                    #     "pid" : proc.pid,
                    #     "time_waited" : waited_time,
                    #     "waited_time_limit" : waited_time_limit
                    #     })

        print(stdoutdata)
        print(stderrdata)
        return {
            "success" : proc.returncode == 0,
            "return_code" : proc.returncode
            }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="loop.py", description="Plot Ray workloads")
    parser.add_argument("--docker-image", default="ray-project/deploy", help="docker image")
    parser.add_argument("--mem-size", help="memory size")
    parser.add_argument("--shm-size", default="1G", help="shared memory size")
    parser.add_argument("--num-workers", default=4, type= int, help="number of workers")
    parser.add_argument("--num-nodes", default=1, type=int, help="number of instances")
    parser.add_argument("workload", help="workload script")
    args = parser.parse_args()

    d = DockerRunner()
    d.start_ray(mem_size=args.mem_size, shm_size=args.shm_size, num_workers=args.num_workers, num_nodes=args.num_nodes, docker_image=args.docker_image)
    try:
        time.sleep(2)

        run_result = d.run_test(args.workload)
    finally:
        d.stop_ray()

    if "success" in run_result and run_result["success"]:
        sys.exit(0)
    else:
        sys.exit(1)