import argparse
import ray

from ray._private.test_utils import get_and_run_node_killer


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    parser.add_argument("--max-nodes-to-kill", type=int, default=2)
    parser.add_argument(
        "--no-start",
        action="store_true",
        default=False,
        help=(
            "If set, node killer won't be starting to kill nodes when "
            "the script is done. Driver needs to manually "
            "obtain the node killer handle and invoke run method to "
            "start killing nodes. If not set, as soon as "
            "the script is done, nodes will be killed every "
            "--node-kill-interval seconds."
        ),
    )
    return parser.parse_known_args()


def main():
    """Start the chaos testing.

    Currently chaos testing only covers random node failures.
    """
    args, _ = parse_script_args()
    ray.init(address="auto")
    get_and_run_node_killer(
        args.node_kill_interval,
        namespace="release_test_namespace",
        lifetime="detached",
        no_start=args.no_start,
        max_nodes_to_kill=args.max_nodes_to_kill,
    )
    print("Successfully deployed a node killer.")


main()
