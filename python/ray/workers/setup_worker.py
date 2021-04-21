import argparse

from ray._private.utils import import_attr
parser = argparse.ArgumentParser(
    description=(
        "Set up the environment for a Ray worker and launch the worker."))

parser.add_argument(
    "--worker-setup-hook",
    type=str,
    help="the module path to a Python function to run to set up the "
    "environment for a worker and launch the worker.")

args, remaining_args = parser.parse_known_args()

setup = import_attr(args.worker_setup_hook)

setup(remaining_args)
