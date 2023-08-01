#!/usr/bin/env python

import argparse
import json
import logging
import os
import time
import subprocess
import tempfile
import shutil
import shlex
import ray
import itertools


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ray.init(address="auto")


def all_node_ids():
    nodes = ray.nodes()
    return [node["NodeID"] for node in nodes]


def create_file_and_assert_n_times(n):
    """
    Prepares a working dir that only contains 1 Python Ray job file.
    Then in each step:
    - create n.txt
    - submit the job with runtime_env to assert the file content matches and the number
        of files created so far.
    - run ray CLI to make sure we can see all these runtime envs.

    The tasks are scheduled to each node in a round robin style.
    """
    node_ids = itertools.cycle(all_node_ids())

    with tempfile.TemporaryDirectory() as temp_dir:
        job_file_dir = os.path.dirname(os.path.abspath(__file__))
        job_file = os.path.join(job_file_dir, "test_many_runtime_envs.py")
        shutil.copy(job_file, temp_dir)
        for i in range(n):
            file_name = os.path.join(temp_dir, f"{i}.txt")
            content = f"this is the {i}th file."
            with open(file_name, "w") as file:
                file.write(content)
            subprocess.check_call(
                f"ray job submit --working-dir {temp_dir} -- "
                " python test_many_runtime_envs.py "
                f"--file-name {file_name} "
                f"--expected-content {shlex.quote(content)} "
                f"--expected-file-count-in-working-dir {i + 2}"
                f"--node_id {next(node_ids)}",
                shell=True,
            )


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-runtime-envs", type=int, default=100)
    return parser.parse_known_args()


if __name__ == "__main__":
    args, unknown = parse_script_args()

    result = {"success": 0}

    start_time = time.time()
    create_file_and_assert_n_times(args.num_runtime_envs)
    end_time = time.time()

    result["success"] = 1

    print("PASSED.")

    logger.info(
        f"Finished {args.num_runtime_envs} rounds "
        f"after {end_time - start_time} seconds."
    )

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_put:
        out_put.write(json.dumps(result))
