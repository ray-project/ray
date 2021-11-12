import argparse
import logging
import asyncio
import grpc
import ray

from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc

from ray._private.test_utils import get_and_run_node_killer


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-kill-interval", type=int, default=60)
    parser.add_argument(
        "--no-start",
        action="store_true",
        default=False,
        help=("If set, node killer won't be started when "
              "the script is done. Script needs to manually "
              "obtain the node killer handle and invoke run method to "
              "start a node killer."))
    return parser.parse_known_args()


def main():
    args, _ = parse_script_args()
    ray.init(address="auto")
    get_and_run_node_killer(
        args.node_kill_interval,
        namespace="release_test_namespace",
        lifetime="detached",
        no_start=args.no_start)
    print("Successfully deployed a node killer.")


main()
