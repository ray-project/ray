import argparse
import csv
from functools import reduce
import io
import logging
import os
import random
import subprocess
from typing import List

import numpy as np
import ray
from ray import ObjectRef

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
        "--num_parts",
        default=1000,
        type=int,
        help="number of partitions (tasks)",
    )
    parser.add_argument(
        "-s",
        "--part_size",
        default=int(1e9),
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--reducer_batch_num_records",
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
    ret = PartitionInfo(part_id, node, filepath)
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
            PartitionInfo(int(part_id), node, path)
            for part_id, node, path in reader
        ]


def _load_partition(path: Path) -> io.BytesIO:
    with open(path, "rb") as fin:
        return io.BytesIO(fin.read())


@ray.remote
def mapper(args: Args, mapper_id: PartId, path: Path) -> List[ObjectRef]:
    logging_utils.init()
    task_id = f"M-{mapper_id}"
    logging.info(f"{task_id} starting")
    part = _load_partition(path)
    logging.info(f"{task_id} done")
    buf = part.getbuffer()
    N = args.num_parts
    offset = 0
    size = int(np.ceil(buf.nbytes / N))
    chunks = []
    for _ in range(N):
        chunks.append((offset, size))
        offset += size
    # TODO: can we avoid copying here?
    return [ray.put(buf[offset:offset + size]) for offset, size in chunks]


@ray.remote
def reducer(args: Args, reducer_id: PartId, *chunks) -> PartitionInfo:
    logging_utils.init()
    task_id = f"R-{reducer_id}"
    logging.info(f"{task_id} starting")
    chunks = ray.get(list(chunks))
    logging.info(f"{task_id} done")
    # Write output to EBS
    node = ray.worker.global_worker.node_ip_address
    mnt = random.choice(constants.WORKER_EBS_MOUNTS)
    filepath = _get_part_path(mnt, reducer_id, "output")
    ret = PartitionInfo(reducer_id, node, filepath)
    with open(filepath, "wb") as fout:
        for chunk in chunks:
            fout.write(chunk)
    return ret


def sort_main(args: Args):
    N = args.num_parts
    partitions = _load_input_manifest()
    mapper_results = np.empty((N, N), dtype=object)
    for part_id, node, path in partitions:
        mapper_results[part_id, :] = mapper.options(
            num_returns=N, resources={f"node:{node}": 1e-3}
        ).remote(
            args, part_id, path
        )  # yapf: disable

    print(mapper_results)
    reducer_results = []
    for r in range(N):
        chunks = mapper_results[:, r].tolist()
        print(r, chunks)
        ret = reducer.remote(args, r, *chunks)
        reducer_results.append(ret)

    output_parts = ray.get(reducer_results)
    with open(constants.OUTPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerows(output_parts)


# ------------------------------------------------------------
#     Validate Output
# ------------------------------------------------------------


def validate_output(args: Args):
    print("validated output")


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
