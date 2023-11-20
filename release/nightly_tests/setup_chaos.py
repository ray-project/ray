import argparse
import ray

from ray._private.test_utils import (
    get_and_run_resource_killer,
    NodeKillerActor,
    WorkerKillerActor,
)


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill-workers", action="store_true", default=False)
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
            "'no-start' is set.",
        ),
    )
    parser.add_argument(
        "--task-names",
        nargs="*",
        default=[],
    )
    return parser.parse_known_args()


def task_filter(task_names):
    if task_names == []:
        return lambda _: True

    def _filter_fn(task):
        return task.name in task_names

    return _filter_fn


def main():
    """Start the chaos testing.

    Currently chaos testing only covers random node failures.
    """
    args, _ = parse_script_args()
    ray.init(address="auto")
    if args.kill_workers:
        resource_killer_cls = WorkerKillerActor
    else:
        resource_killer_cls = NodeKillerActor

    get_and_run_resource_killer(
        resource_killer_cls,
        args.kill_interval,
        namespace="release_test_namespace",
        lifetime="detached",
        no_start=args.no_start,
        max_to_kill=args.max_to_kill,
        kill_delay_s=args.kill_delay,
        task_filter=task_filter(args.task_names),
    )
    print(
        f"Successfully deployed a {'worker' if args.kill_workers else 'node'} killer."
    )


main()
