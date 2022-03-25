import argparse
import os
import sys

parser = argparse.ArgumentParser(
    description=("Set up the environment for a Ray worker and launch the worker.")
)

parser.add_argument(
    "--worker-setup-hook",
    type=str,
    help="the module path to a Python function to run to set up the "
    "environment for a worker and launch the worker.",
)
parser.add_argument(
    "--serialized-runtime-env", type=str, help="the serialized parsed runtime env dict"
)

parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context",
)

parser.add_argument(
    "--allocated-instances-serialized-json",
    type=str,
    help="the worker allocated resource",
)

# The worker is not set up yet, so we can't get session_dir from the worker.
parser.add_argument(
    "--session-dir", type=str, help="the directory for the current session"
)

parser.add_argument("--language", type=str, help="the language type of the worker")

args, remaining_args = parser.parse_known_args()

py_executable: str = sys.executable
command_str = " ".join([f"exec {py_executable}"] + remaining_args)
child_pid = os.fork()
if child_pid == 0:
    # child process
    os.execvp("bash", ["bash", "-c", command_str])
os.waitpid(child_pid, 0)
