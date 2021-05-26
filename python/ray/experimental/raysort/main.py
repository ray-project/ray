import argparse
import csv
import logging
import os
import random
import subprocess

import numpy as np
import ray

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils
from ray.experimental.raysort.types import ByteCount, RecordCount, PartId, PartitionInfo


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num-parts",
        default=100,
        type=int,
        help="number of partitions (tasks)",
    )
    parser.add_argument(
        "--part-size",
        default=10e9,
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--reducer-batch-num-records",
        default=1e6,
        type=RecordCount,
        help="number of bytes to buffer before writing the output to EBS",
    )
    # Which tasks to run?
    tasks_group = parser.add_argument_group(
        "tasks to run", "if no task is specified, will run all tasks")
    tasks = ["generate_input", "sort", "validate_output"]
    for task in tasks:
        tasks_group.add_argument(
            f"--{task}", action="store_true", help=f"run task {task}")

    args = parser.parse_args()
    # Derive additional arguments.
    args.part_num_records = constants.bytes_to_records(args.part_size)
    args.total_data_size = args.num_parts * args.part_size
    args.total_num_records = constants.bytes_to_records(
        args.num_parts * args.part_size)
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


def _get_part_path(mnt, part_id, kind="input"):
    assert kind in {"input", "output"}
    dir_fmt = constants.DATA_DIR_FMT[kind]
    dirpath = dir_fmt.format(mnt=mnt)
    os.makedirs(dirpath, exist_ok=True)
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


@ray.remote
def generate_part(part_id: PartId, size: RecordCount,
                  offset: RecordCount) -> PartitionInfo:
    logging_utils.init()
    mnt = random.choice(constants.WORKER_EBS_MOUNTS)
    filepath = _get_part_path(mnt, part_id, "input")
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", filepath],
        check=True,
    )
    logging.info(f"Generated input {filepath} containing {size:,} records")
    node = ray.worker.global_worker.node_ip_address
    return PartitionInfo(part_id, node, mnt, filepath)


def generate_input(args):
    size = args.part_num_records
    offset = 0
    tasks = []
    for part_id in range(args.num_parts):
        tasks.append(generate_part.remote(part_id, size, offset))
        offset += size
    assert offset == args.total_num_records, args
    logging.info(f"Generating {len(tasks)} partitions")
    parts = ray.get(tasks)
    with open(constants.INPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerow(["part_id", "node", "mnt", "path"])
        writer.writerows(parts)


def init(args):
    ray.init(address="auto")
    logging_utils.init()
    logging.info(args)
    os.makedirs(constants.WORK_DIR, exist_ok=True)


def main():
    args = get_args()
    init(args)

    if args.generate_input:
        generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        validate_output(args)


if __name__ == "__main__":
    main()
