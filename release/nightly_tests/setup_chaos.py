import argparse
from ray.util.state.api import StateApiClient
from ray.util.state.common import ListApiOptions, StateResource
import ray

from ray._private.test_utils import (
    get_and_run_resource_killer,
    RayletKiller,
    WorkerKillerActor,
    EC2InstanceTerminator,
)


def parse_script_args():
    parser = argparse.ArgumentParser()

    # '--kill-workers' to be deprecated in favor of '--chaos'
    parser.add_argument("--kill-workers", action="store_true", default=False)

    parser.add_argument(
        "--chaos",
        type=str,
        default="",
        help=(
            "Chaos to inject into the test environment. "
            "Options: KillRaylet, KillWorker, TerminateEC2Instance."
        ),
    )

    parser.add_argument("--kill-interval", type=int, default=60)
    parser.add_argument("--max-to-kill", type=int, default=2)
    parser.add_argument(
        "--no-start",
        action="store_true",
        default=False,
        help=(
            "If set, resource killer won't be starting to kill resources when "
            "the script is done. Driver needs to manually "
            "obtain the resource killer handle and invoke run method to "
            "start killing nodes. If not set, as soon as "
            "the script is done, resources will be killed every "
            "--kill-interval seconds."
        ),
    )
    parser.add_argument(
        "--kill-delay",
        type=int,
        default=0,
        help=(
            "Seconds to wait before node killer starts killing nodes. No-op if "
            "'no-start' is set."
        ),
    )
    parser.add_argument(
        "--task-names",
        nargs="*",
        default=[],
    )
    return parser.parse_known_args()


def task_filter(task_names):
    def _task_filter():
        if not task_names:
            return lambda _: True

        def _filter_fn(task):
            return task.name in task_names

        return _filter_fn

    return _task_filter


def task_node_filter(task_names):
    def _task_node_filter():
        if not task_names:
            return lambda _: True

        tasks = StateApiClient().list(
            StateResource.TASKS, options=ListApiOptions(), raise_on_missing_output=False
        )
        filtered_tasks = list(filter(lambda task: task.name in task_names, tasks))
        nodes_with_filtered_tasks = {task.node_id for task in filtered_tasks}

        def _filter_fn(node):
            return node["NodeID"] in nodes_with_filtered_tasks

        return _filter_fn

    return _task_node_filter


def get_chaos_killer(args):
    if args.chaos != "":
        chaos_type = args.chaos
    elif args.kill_workers:
        chaos_type = "KillWorker"
    else:
        chaos_type = "KillRaylet"  # default

    if chaos_type == "KillRaylet":
        return RayletKiller, task_node_filter(args.task_names)
    elif chaos_type == "KillWorker":
        return WorkerKillerActor, task_filter(args.task_names)
    elif chaos_type == "TerminateEC2Instance":
        return EC2InstanceTerminator, task_node_filter(args.task_names)
    else:
        raise ValueError(f"Chaos type {chaos_type} not supported.")


def main():
    """Start the chaos testing.

    Currently, chaos testing only covers random node failures.
    """
    args, _ = parse_script_args()
    ray.init(address="auto")
    resource_killer_cls, kill_filter_fn = get_chaos_killer(args)

    get_and_run_resource_killer(
        resource_killer_cls,
        args.kill_interval,
        namespace="release_test_namespace",
        lifetime="detached",
        no_start=args.no_start,
        max_to_kill=args.max_to_kill,
        kill_delay_s=args.kill_delay,
        kill_filter_fn=kill_filter_fn,
    )
    print(
        f"Successfully deployed a {'worker' if args.kill_workers else 'node'} killer."
    )


main()
