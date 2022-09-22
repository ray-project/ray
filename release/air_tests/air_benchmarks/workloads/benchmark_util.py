import os
import socket
import subprocess
from collections import defaultdict
from contextlib import closing
from pathlib import Path

import ray
from typing import Any, List, Dict, Union, Callable


def _schedule_remote_fn_on_node(node_ip: str, remote_fn, *args, **kwargs):
    return remote_fn.options(resources={f"node:{node_ip}": 0.01}).remote(
        *args,
        **kwargs,
    )


def schedule_remote_fn_on_all_nodes(
    remote_fn, exclude_head: bool = False, *args, **kwargs
):
    head_ip = ray.util.get_node_ip_address()

    futures = []
    for node in ray.nodes():
        if not node["Alive"]:
            continue

        node_ip = node["NodeManagerAddress"]

        if exclude_head and node_ip == head_ip:
            continue

        future = _schedule_remote_fn_on_node(node_ip, remote_fn, *args, **kwargs)
        futures.append(future)
    return futures


@ray.remote
def _write(stream: bytes, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        f.write(stream)


def upload_file_to_all_nodes(path: str):
    path = os.path.abspath(path)

    with open(path, "rb") as f:
        stream = f.read()

    futures = schedule_remote_fn_on_all_nodes(
        _write, exclude_head=True, stream=stream, path=path
    )
    return ray.get(futures)


@ray.remote
def _run_command(cmd: str):
    return subprocess.check_call(cmd)


def run_command_on_all_nodes(cmd: List[str]):
    futures = schedule_remote_fn_on_all_nodes(_run_command, cmd=cmd)
    return ray.get(futures)


@ray.remote
class CommandRunner:
    def run_command(self, cmd: str):
        return subprocess.check_call(cmd)

    def run_fn(self, fn: Callable, *args, **kwargs):
        return fn(*args, **kwargs)


def create_actors_with_options(
    num_actors: int,
    resources: Dict[str, Union[float, int]],
    runtime_env: Dict[str, Any] = None,
) -> List[ray.actor.ActorHandle]:
    num_cpus = resources.pop("CPU", 1)
    num_gpus = resources.pop("GPU", 0)

    options = {"num_cpus": num_cpus, "num_gpus": num_gpus, "resources": resources}

    if runtime_env:
        options["runtime_env"] = runtime_env

    return [CommandRunner.options(**options).remote() for _ in range(num_actors)]


def run_commands_on_actors(actors: List[ray.actor.ActorHandle], cmds: List[List[str]]):
    assert len(actors) == len(cmds)
    futures = []
    for actor, cmd in zip(actors, cmds):
        futures.append(actor.run_command.remote(cmd))
    return ray.get(futures)


def run_fn_on_actors(
    actors: List[ray.actor.ActorHandle], fn: Callable, *args, **kwargs
):
    futures = []
    for actor in actors:
        futures.append(actor.run_fn.remote(fn, *args, **kwargs))
    return ray.get(futures)


def get_ip_port_actors(actors: List[ray.actor.ActorHandle]) -> List[str]:
    # We need this wrapper to avoid deserialization issues with benchmark_util.py

    def get_ip_port():
        ip = ray.util.get_node_ip_address()
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("localhost", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = s.getsockname()[1]
        return ip, port

    return run_fn_on_actors(actors=actors, fn=get_ip_port)


def get_gpu_ids_actors(actors: List[ray.actor.ActorHandle]) -> List[List[int]]:
    # We need this wrapper to avoid deserialization issues with benchmark_util.py

    def get_gpu_ids():
        return ray.get_gpu_ids()

    return run_fn_on_actors(actors=actors, fn=get_gpu_ids)


def map_ips_to_gpus(ips: List[str], gpus: List[List[int]]):
    assert len(ips) == len(gpus)

    map = defaultdict(set)
    for ip, gpu in zip(ips, gpus):
        map[ip].update(set(gpu))
    return {ip: sorted(gpus) for ip, gpus in map.items()}


def set_cuda_visible_devices(
    actors: List[ray.actor.ActorHandle],
    actor_ips: List[str],
    ip_to_gpus: Dict[str, set],
):
    assert len(actors) == len(actor_ips)

    def set_env(key: str, val: str):
        os.environ[key] = val

    futures = []
    for actor, ip in zip(actors, actor_ips):
        assert ip in ip_to_gpus

        gpu_str = ",".join([str(device) for device in sorted(ip_to_gpus[ip])])
        future = actor.run_fn.remote(set_env, "CUDA_VISIBLE_DEVICES", gpu_str)
        futures.append(future)

    ray.get(futures)
