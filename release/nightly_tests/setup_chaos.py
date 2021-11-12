import argparse

import ray

from ray._private.test_utils import get_and_run_node_killer


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    return parser.parse_known_args()


def main():
    args, _ = parse_script_args()
    ray.init(address="auto")
    get_and_run_node_killer(
        args.node_kill_interval, namespace="release_test_namespace")
    print("Successfully deployed a node killer.")


main()
