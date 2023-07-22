"""
This script runs once and expects certain properties from the argparse.
"""
#!/usr/bin/env python

import argparse
import logging
import os

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")


@ray.remote
def assert_file_content(file_name, expected_content):
    with open(file_name, 'r') as file:
        actual_content = file.read()
    if actual_content != expected_content:
        raise ValueError(
            f"expected {expected_content}, got {actual_content}")


@ray.remote
def assert_n_files_in_working_dir(n):
    files = os.listdir()
    num_files = len(files)
    if num_files != n:
        raise ValueError(
            f"Expected {n} files in working dir, but found {num_files} files.")


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-name", type=str)
    parser.add_argument("--expected-content", type=str)
    parser.add_argument("--expected-file-count-in-working-dir", type=int)
    return parser.parse_known_args()


if __name__ == "__main__":
    args, unknown = parse_script_args()

    ray.get([
        assert_file_content.remote(args.file_name, args.expected_content),
        assert_n_files_in_working_dir.remote(args.expected_file_count_in_working_dir)
    ])
