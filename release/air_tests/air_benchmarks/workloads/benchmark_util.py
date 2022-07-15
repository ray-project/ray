import os
import subprocess
from pathlib import Path

import ray
from typing import List, Dict, Union, Callable


def _schedule_remote_fn_on_node(node_ip: str, remote_fn, *args, **kwargs):
    return remote_fn.options(resources={f"node:{node_ip}": 0.01}).remote(
        *args,
        **kwargs,
    )


def _schedule_remote_fn_on_all_nodes(
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

    futures = _schedule_remote_fn_on_all_nodes(
        _write, exclude_head=True, stream=stream, path=path
    )
    return ray.get(futures)


@ray.remote
def _run_command(cmd: str):
    return subprocess.check_call(cmd)


def run_command_on_all_nodes(cmd: List[str]):
    futures = _schedule_remote_fn_on_all_nodes(_run_command, cmd=cmd)
    return ray.get(futures)


def run_commands_with_resources(
    cmds: List[List[str]], resources: Dict[str, Union[float, int]]
):
    num_cpus = resources.pop("CPU", 1)
    num_gpus = resources.pop("GPU", 0)
    futures = []
    for cmd in cmds:
        future = _run_command.options(
            num_cpus=num_cpus, num_gpus=num_gpus, resources=resources
        ).remote(cmd=cmd)
        futures.append(future)
    return ray.get(futures)


@ray.remote
class CommandRunner:
    def run_command(self, cmd: str):
        return subprocess.check_call(cmd)

    def run_fn(self, fn: Callable, *args, **kwargs):
        return fn(*args, **kwargs)


def create_actors_with_resources(
    num_actors: int, resources: Dict[str, Union[float, int]]
) -> List[ray.actor.ActorHandle]:
    num_cpus = resources.pop("CPU", 1)
    num_gpus = resources.pop("GPU", 0)

    return [
        CommandRunner.options(
            num_cpus=num_cpus, num_gpus=num_gpus, resources=resources
        ).remote()
        for _ in range(num_actors)
    ]


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
