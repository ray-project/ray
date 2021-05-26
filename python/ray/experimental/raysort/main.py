import argparse
import csv
import io
import logging
import os
import random
import subprocess
from typing import List

import numpy as np
import ray

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils
from ray.experimental.raysort.types import (
    ByteCount,
    RecordCount,
    PartId,
    PartitionInfo,
    Path,
)

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------

Args = argparse.Namespace


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
        default=int(1e9),
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--reducer-batch-num-records",
        default=int(1e6),
        type=RecordCount,
        help="number of bytes to buffer before writing the output to EBS",
    )
    # Which tasks to run?
    tasks_group = parser.add_argument_group(
        "tasks to run", "if no task is specified, will run all tasks")
    tasks = ["generate_input", "sort", "validate_output"]
    for task in tasks:
        tasks_group.add_argument(f"--{task}",
                                 action="store_true",
                                 help=f"run task {task}")

    args = parser.parse_args()
    # Derive additional arguments.
    args.part_num_records = constants.bytes_to_records(args.part_size)
    args.total_data_size = args.num_parts * args.part_size
    args.total_num_records = constants.bytes_to_records(args.num_parts *
                                                        args.part_size)
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _get_part_path(mnt: Path, part_id: PartId, kind="input") -> Path:
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
    node = ray.worker.global_worker.node_ip_address
    mnt = random.choice(constants.WORKER_EBS_MOUNTS)
    filepath = _get_part_path(mnt, part_id, "input")
    ret = PartitionInfo(part_id, node, mnt, filepath)
    logging.info(ret)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", filepath],
        check=True)
    return ret


def generate_input(args: Args):
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
        writer.writerows(parts)


# ------------------------------------------------------------
#     Sort
# ------------------------------------------------------------


def _load_input_manifest() -> List[PartitionInfo]:
    with open(constants.INPUT_MANIFEST_FILE) as fin:
        reader = csv.reader(fin)
        return [
            PartitionInfo(int(part_id), node, mnt, path)
            for part_id, node, mnt, path in reader
        ]


def _load_partition(path: Path) -> io.BytesIO:
    with open(path, "rb") as fin:
        return io.BytesIO(fin.read())


@ray.remote
def mapper(args: Args, part_id: PartId, path: Path) -> List[np.ndarray]:
    logging_utils.init()
    task_id = f"M-{part_id}"
    logging.info(f"{task_id} starting")
    part = _load_partition(path)
    print(part.getbuffer().nbytes)
    logging.info(f"{task_id} done")
    # TODO: can we avoid copying here?
    return []


def sort_main(args: Args):
    N = args.num_parts
    partitions = _load_input_manifest()
    mapper_results = np.empty((N, N), dtype=object)
    for part_id, node, _, path in partitions:
        res = {f"node:{node}": 1e-3}
        mapper_results[part_id, :] = mapper.options(num_returns=N,
                                                    resources=res).remote(
                                                        args, part_id, path)


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def init(args: Args):
    ray.init(address="auto")
    logging_utils.init()
    logging.info(args)
    logging.info(ray.available_resources())
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
