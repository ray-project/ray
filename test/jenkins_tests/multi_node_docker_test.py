from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import datetime
import os
import random
import re
import signal
import subprocess
import sys


# This is duplicated from ray.utils so that we do not have to introduce a
# dependency on Ray to run this file.
def decode(byte_str):
    """Make this unicode in Python 3, otherwise leave it as bytes."""
    if not isinstance(byte_str, bytes):
        raise ValueError("The argument must be a bytes object.")
    if sys.version_info >= (3, 0):
        return byte_str.decode("ascii")
    else:
        return byte_str


def wait_for_output(proc):
    """This is a convenience method to parse a process's stdout and stderr.

    Args:
        proc: A process started by subprocess.Popen.

    Returns:
        A tuple of the stdout and stderr of the process as strings.
    """
    try:
        # NOTE: This test must be run with Python 3.
        stdout_data, stderr_data = proc.communicate(timeout=200)
    except subprocess.TimeoutExpired:
        # Timeout: kill the process.
        # Get the remaining message from PIPE for debugging purpose.
        print("Killing process because it timed out.")
        proc.kill()
        stdout_data, stderr_data = proc.communicate()

    if stdout_data is not None:
        try:
            # NOTE(rkn): This try/except block is here because I once saw an
            # exception raised here and want to print more information if that
            # happens again.
            stdout_data = decode(stdout_data)
        except UnicodeDecodeError:
            raise Exception("Failed to decode stdout_data:", stdout_data)

    if stderr_data is not None:
        try:
            # NOTE(rkn): This try/except block is here because I once saw an
            # exception raised here and want to print more information if that
            # happens again.
            stderr_data = decode(stderr_data)
        except UnicodeDecodeError:
            raise Exception("Failed to decode stderr_data:", stderr_data)

    return stdout_data, stderr_data


class DockerRunner(object):
    """This class manages the logistics of running multiple nodes in Docker.

    This class is used for starting multiple Ray nodes within Docker, stopping
    Ray, running a workload, and determining the success or failure of the
    workload.

    Attributes:
        head_container_id: The ID of the docker container that runs the head
            node.
        worker_container_ids: A list of the docker container IDs of the Ray
            worker nodes.
        head_container_ip: The IP address of the docker container that runs the
            head node.
    """

    def __init__(self, use_raylet):
        """Initialize the DockerRunner."""
        self.head_container_id = None
        self.worker_container_ids = []
        self.head_container_ip = None
        self.use_raylet = use_raylet

    def _get_container_id(self, stdout_data):
        """Parse the docker container ID from stdout_data.

        Args:
            stdout_data: This should be a string with the standard output of a
                call to a docker command.

        Returns:
            The container ID of the docker container.
        """
        p = re.compile("([0-9a-f]{64})\n")
        m = p.match(stdout_data)
        if m is None:
            return None
        else:
            return m.group(1)

    def _get_container_ip(self, container_id):
        """Get the IP address of a specific docker container.

        Args:
            container_id: The docker container ID of the relevant docker
                container.

        Returns:
            The IP address of the container.
        """
        proc = subprocess.Popen(
            [
                "docker", "inspect",
                "--format={{.NetworkSettings.Networks.bridge"
                ".IPAddress}}", container_id
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout_data, _ = wait_for_output(proc)
        p = re.compile("([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})")
        m = p.match(stdout_data)
        if m is None:
            raise RuntimeError("Container IP not found.")
        else:
            return m.group(1)

    def _start_head_node(self, docker_image, mem_size, shm_size,
                         num_redis_shards, num_cpus, num_gpus,
                         development_mode):
        """Start the Ray head node inside a docker container."""
        mem_arg = ["--memory=" + mem_size] if mem_size else []
        shm_arg = ["--shm-size=" + shm_size] if shm_size else []
        volume_arg = ([
            "-v", "{}:{}".format(
                os.path.dirname(os.path.realpath(__file__)),
                "/ray/test/jenkins_tests")
        ] if development_mode else [])

        command = (["docker", "run", "-d"] + mem_arg + shm_arg + volume_arg + [
            docker_image, "ray", "start", "--head", "--block",
            "--redis-port=6379",
            "--num-redis-shards={}".format(num_redis_shards),
            "--num-cpus={}".format(num_cpus), "--num-gpus={}".format(num_gpus),
            "--no-ui"
        ])
        if self.use_raylet:
            command.append("--use-raylet")
        print("Starting head node with command:{}".format(command))

        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_data, _ = wait_for_output(proc)
        container_id = self._get_container_id(stdout_data)
        if container_id is None:
            raise RuntimeError("Failed to find container ID.")
        self.head_container_id = container_id
        self.head_container_ip = self._get_container_ip(container_id)

    def _start_worker_node(self, docker_image, mem_size, shm_size, num_cpus,
                           num_gpus, development_mode):
        """Start a Ray worker node inside a docker container."""
        mem_arg = ["--memory=" + mem_size] if mem_size else []
        shm_arg = ["--shm-size=" + shm_size] if shm_size else []
        volume_arg = ([
            "-v", "{}:{}".format(
                os.path.dirname(os.path.realpath(__file__)),
                "/ray/test/jenkins_tests")
        ] if development_mode else [])
        command = (["docker", "run", "-d"] + mem_arg + shm_arg + volume_arg + [
            "--shm-size=" + shm_size, docker_image, "ray", "start", "--block",
            "--redis-address={:s}:6379".format(self.head_container_ip),
            "--num-cpus={}".format(num_cpus), "--num-gpus={}".format(num_gpus)
        ])
        if self.use_raylet:
            command.append("--use-raylet")
        print("Starting worker node with command:{}".format(command))
        proc = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_data, _ = wait_for_output(proc)
        container_id = self._get_container_id(stdout_data)
        if container_id is None:
            raise RuntimeError("Failed to find container id")
        self.worker_container_ids.append(container_id)

    def start_ray(self,
                  docker_image=None,
                  mem_size=None,
                  shm_size=None,
                  num_nodes=None,
                  num_redis_shards=1,
                  num_cpus=None,
                  num_gpus=None,
                  development_mode=None):
        """Start a Ray cluster within docker.

        This starts one docker container running the head node and
        num_nodes - 1 docker containers running the Ray worker nodes.

        Args:
            docker_image: The docker image to use for all of the nodes.
            mem_size: The amount of memory to start each docker container with.
                This will be passed into `docker run` as the --memory flag. If
                this is None, then no --memory flag will be used.
            shm_size: The amount of shared memory to start each docker
                container with. This will be passed into `docker run` as the
                `--shm-size` flag.
            num_nodes: The number of nodes to use in the cluster (this counts
                the head node as well).
            num_redis_shards: The number of Redis shards to use on the head
                node.
            num_cpus: A list of the number of CPUs to start each node with.
            num_gpus: A list of the number of GPUs to start each node with.
            development_mode: True if you want to mount the local copy of
                test/jenkins_test on the head node so we can avoid rebuilding
                docker images during development.
        """
        assert len(num_cpus) == num_nodes
        assert len(num_gpus) == num_nodes

        # Launch the head node.
        self._start_head_node(docker_image, mem_size, shm_size,
                              num_redis_shards, num_cpus[0], num_gpus[0],
                              development_mode)
        # Start the worker nodes.
        for i in range(num_nodes - 1):
            self._start_worker_node(docker_image, mem_size, shm_size,
                                    num_cpus[1 + i], num_gpus[1 + i],
                                    development_mode)

    def _stop_node(self, container_id):
        """Stop a node in the Ray cluster."""
        proc = subprocess.Popen(
            ["docker", "kill", container_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout_data, _ = wait_for_output(proc)
        stopped_container_id = self._get_container_id(stdout_data)
        if not container_id == stopped_container_id:
            raise Exception("Failed to stop container {}."
                            .format(container_id))

        proc = subprocess.Popen(
            ["docker", "rm", "-f", container_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout_data, _ = wait_for_output(proc)
        removed_container_id = self._get_container_id(stdout_data)
        if not container_id == removed_container_id:
            raise Exception("Failed to remove container {}."
                            .format(container_id))

        print(
            "stop_node", {
                "container_id": container_id,
                "is_head": container_id == self.head_container_id
            })

    def stop_ray(self):
        """Stop the Ray cluster."""
        success = True

        try:
            self._stop_node(self.head_container_id)
        except Exception:
            success = False

        for container_id in self.worker_container_ids:
            try:
                self._stop_node(container_id)
            except Exception:
                success = False

        return success

    def run_test(self,
                 test_script,
                 num_drivers,
                 driver_locations=None,
                 timeout_seconds=600):
        """Run a test script.

        Run a test using the Ray cluster.

        Args:
            test_script: The test script to run.
            num_drivers: The number of copies of the test script to run.
            driver_locations: A list of the indices of the containers that the
                different copies of the test script should be run on. If this
                is None, then the containers will be chosen randomly.
            timeout_seconds: The amount of time in seconds to wait before
                considering the test to have failed. When the timeout expires,
                this will cause this function to raise an exception.

        Returns:
            A dictionary with information about the test script run.

        Raises:
            Exception: An exception is raised if the timeout expires.
        """
        print("Multi-node docker test started at: {}".format(
            datetime.datetime.now()))
        all_container_ids = (
            [self.head_container_id] + self.worker_container_ids)
        if driver_locations is None:
            driver_locations = [
                random.randrange(0, len(all_container_ids))
                for i in range(num_drivers)
            ]
        print("driver_locations: {}".format(driver_locations))

        # Define a signal handler and set an alarm to go off in
        # timeout_seconds.
        def handler(signum, frame):
            raise RuntimeError("This test timed out after {} seconds."
                               .format(timeout_seconds))

        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout_seconds)

        # Start the different drivers.
        driver_processes = []
        if self.use_raylet:
            use_raylet_env = 1
        else:
            use_raylet_env = 0
        for i in range(len(driver_locations)):
            # Get the container ID to run the ith driver in.
            container_id = all_container_ids[driver_locations[i]]
            command = [
                "docker", "exec", container_id, "/bin/bash",
                "-c", ("RAY_REDIS_ADDRESS={}:6379 RAY_DRIVER_INDEX={} "
                       "RAY_USE_XRAY={} python {}".format(
                           self.head_container_ip, i, use_raylet_env,
                           test_script))
            ]
            print("Starting driver with command {}.".format(test_script))
            # Start the driver.
            p = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            driver_processes.append(p)

        results = []
        for p in driver_processes:
            stdout_data, stderr_data = wait_for_output(p)
            print("STDOUT:")
            print(stdout_data)
            print("STDERR:")
            print(stderr_data)
            results.append({
                "success": p.returncode == 0,
                "return_code": p.returncode
            })

        # Disable the alarm.
        signal.alarm(0)
        print("Multi-node docker test ended at: {}".format(
            datetime.datetime.now()))
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run multinode tests in Docker.")
    parser.add_argument(
        "--docker-image", default="ray-project/deploy", help="docker image")
    parser.add_argument("--mem-size", help="memory size")
    parser.add_argument("--shm-size", default="1G", help="shared memory size")
    parser.add_argument(
        "--num-nodes",
        default=1,
        type=int,
        help="number of nodes to use in the cluster")
    parser.add_argument(
        "--num-redis-shards",
        default=1,
        type=int,
        help=("the number of Redis shards to start on the "
              "head node"))
    parser.add_argument(
        "--num-cpus",
        type=str,
        help=("a comma separated list of values representing "
              "the number of CPUs to start each node with"))
    parser.add_argument(
        "--num-gpus",
        type=str,
        help=("a comma separated list of values representing "
              "the number of GPUs to start each node with"))
    parser.add_argument(
        "--num-drivers", default=1, type=int, help="number of drivers to run")
    parser.add_argument(
        "--driver-locations",
        type=str,
        help=("a comma separated list of indices of the "
              "containers to run the drivers in"))
    parser.add_argument("--test-script", required=True, help="test script")
    parser.add_argument(
        "--development-mode",
        action="store_true",
        help="use local copies of the test scripts")
    parser.add_argument(
        "--use-raylet", action="store_true", help="use raylet mode in Docker")
    args = parser.parse_args()

    # Parse the number of CPUs and GPUs to use for each worker.
    num_nodes = args.num_nodes
    num_cpus = ([int(i) for i in args.num_cpus.split(",")]
                if args.num_cpus is not None else num_nodes * [10])
    num_gpus = ([int(i) for i in args.num_gpus.split(",")]
                if args.num_gpus is not None else num_nodes * [0])

    # Parse the driver locations.
    driver_locations = (None if args.driver_locations is None else
                        [int(i) for i in args.driver_locations.split(",")])

    d = DockerRunner(args.use_raylet)
    d.start_ray(
        docker_image=args.docker_image,
        mem_size=args.mem_size,
        shm_size=args.shm_size,
        num_nodes=num_nodes,
        num_redis_shards=args.num_redis_shards,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        development_mode=args.development_mode)
    try:
        run_results = d.run_test(
            args.test_script,
            args.num_drivers,
            driver_locations=driver_locations)
    finally:
        successfully_stopped = d.stop_ray()

    any_failed = False
    for run_result in run_results:
        if "success" in run_result and run_result["success"]:
            print("RESULT: Test {} succeeded.".format(args.test_script))
        else:
            print("RESULT: Test {} failed.".format(args.test_script))
            any_failed = True

    if any_failed:
        sys.exit(1)
    elif not successfully_stopped:
        print("There was a failure when attempting to stop the containers.")
        sys.exit(1)
    else:
        sys.exit(0)
