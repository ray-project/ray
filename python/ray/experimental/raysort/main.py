import argparse
import csv
from functools import reduce
import io
import logging
import os
import random
import subprocess
from typing import Iterable, List, Union

import numpy as np
import ray
from ray import ObjectRef

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils
from ray.experimental.raysort import sortlib
from ray.experimental.raysort import tracing_utils
from ray.experimental.raysort.types import BlockInfo, ByteCount, RecordCount, PartId, PartitionInfo, Path

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------


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
        default=int(10e9),
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--reducer_batch_num_records",
        default=int(1e6),
        type=RecordCount,
        help="number of bytes to buffer before writing the output to EBS",
    )
    parser.add_argument(
        "--worker_ebs_mounts",
        default=[f"/mnt/nvme{d}" for d in range(4)],
        type=list,
        help=
        "a list of mount paths for persisting results\nthe benchmark requires this to be on EBS, but for now we use local NVMe disks",
    )
    parser.add_argument(
        "--shuffle_only",
        default=False,
        action="store_true",
        help="if set, will only run the shuffle microbenchmark",
    )
    parser.add_argument(
        "--skip_sorting",
        default=False,
        action="store_true",
        help="if set, no sorting is actually performed",
    )
    parser.add_argument(
        "--skip_writing_output",
        default=False,
        action="store_true",
        help="if set, reducers will not write out results to disk",
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


args = get_args()

# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _make_partition_info(part_id: PartId, kind="input") -> PartitionInfo:
    node = ray.worker.global_worker.node_ip_address
    mnt = random.choice(args.worker_ebs_mounts)
    filepath = _get_part_path(mnt, part_id, kind)
    return PartitionInfo(part_id, node, filepath)


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
    pinfo = _make_partition_info(part_id)
    if not args.shuffle_only:
        subprocess.run(
            [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path],
            check=True)
        logging.info(f"Generated input {pinfo}")
    return pinfo


def generate_input():
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


def _load_manifest(path: Path) -> List[PartitionInfo]:
    with open(path) as fin:
        reader = csv.reader(fin)
        return [
            PartitionInfo(int(part_id), node, path)
            for part_id, node, path in reader
        ]


def _load_partition(path: Path) -> np.ndarray:
    return np.fromfile(path, dtype=np.uint8)


def _dummy_sort_and_partition(part: np.ndarray,
                              boundaries: List[int]) -> List[BlockInfo]:
    N = len(boundaries)
    offset = 0
    size = int(np.ceil(part.size / N))
    blocks = []
    for _ in range(N):
        blocks.append((offset, size))
        offset += size
    return blocks


@ray.remote(num_returns=args.num_parts)
def mapper(boundaries: List[int], mapper_id: PartId,
           path: Path) -> List[ObjectRef]:
    logging_utils.init()
    task_id = f"M-{mapper_id} Mapper"
    logging.info(f"{task_id} starting")
    if args.shuffle_only:
        block_size = int(np.ceil(args.part_size / args.num_parts))
        return [
            ray.put(np.empty(block_size, dtype=np.uint8))
            for _ in range(args.num_parts)
        ]

    part = _load_partition(path)
    sort_fn = _dummy_sort_and_partition if args.skip_sorting else sortlib.sort_and_partition
    blocks = sort_fn(part, boundaries)
    logging.info(f"{task_id} saving to object store")
    ret = [ray.put(part[offset:offset + size]) for offset, size in blocks]
    logging.info(f"{task_id} done")
    return ret


def _dummy_merge(blocks: List[Union[np.ndarray, ray.ObjectRef]],
                 _n: int) -> Iterable[memoryview]:
    for block in blocks:
        if isinstance(block, ray.ObjectRef):
            block = ray.get(block)
        yield memoryview(block)


@ray.remote
def reducer(reducer_id: PartId, *blocks) -> PartitionInfo:
    logging_utils.init()
    task_id = f"R-{reducer_id} Reducer"
    logging.info(f"{task_id} starting")
    # if args.shuffle_only:
    #     total = 0
    #     for block in blocks:
    #         block = ray.get(block)
    #         total += block.size
    #     return (total,)
    blocks = ray.get(list(blocks))
    if args.shuffle_only:
        return (sum(block.size for block in blocks), )
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(blocks, args.reducer_batch_num_records)
    pinfo = _make_partition_info(reducer_id, "output")
    if args.skip_writing_output:
        total_bytes = 0
        for datachunk in merger:
            total_bytes += len(datachunk)
    else:
        with open(pinfo.path, "wb") as fout:
            for datachunk in merger:
                fout.write(datachunk)
    logging.info(f"{task_id} done")
    return pinfo


@tracing_utils.timeit("sorting")
def sort_main():
    N = args.num_parts
    partitions = _load_manifest(constants.INPUT_MANIFEST_FILE)
    boundaries = sortlib.get_boundaries(N)
    mapper_results = np.empty((N, N), dtype=object)
    for part_id, node, path in partitions:
        if not args.shuffle_only:
            opt = {
                "memory": args.part_size,
                "resources": {
                    f"node:{node}": 1 / args.num_parts
                },
            }
        else:
            opt = {}
        mapper_results[part_id, :] = mapper.options(**opt).remote(
            boundaries, part_id, path)

    reducer_results = []
    for r in range(N):
        if not args.shuffle_only:
            opt = {"memory": args.part_size}
        else:
            opt = {}
        blocks = mapper_results[:, r].tolist()
        ret = reducer.options(**opt).remote(r, *blocks)
        reducer_results.append(ret)

    reducer_results = ray.get(reducer_results)
    with open(constants.OUTPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerows(reducer_results)


# ------------------------------------------------------------
#     Validate Output
# ------------------------------------------------------------


@ray.remote
def validate_part(path: Path):
    logging_utils.init()
    proc = subprocess.run([constants.VALSORT_PATH, path], capture_output=True)
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {path}")
    logging.info(f"Validated output {path}")


def validate_output():
    if args.shuffle_only:
        return
    partitions = _load_manifest(constants.OUTPUT_MANIFEST_FILE)
    tasks = []
    for _, node, path in partitions:
        tasks.append(
            validate_part.options(resources={
                f"node:{node}": 1 / args.num_parts
            }).remote(path))
    logging.info(f"Validating {len(tasks)} partitions")
    ray.get(tasks)
    logging.info("All done!")


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def init():
    ray.init(address="auto")
    logging_utils.init()
    logging.info(args)
    logging.info(ray.available_resources())
    os.makedirs(constants.WORK_DIR, exist_ok=True)


def main():
    init()

    if args.generate_input:
        generate_input()

    if args.sort:
        sort_main()

    if args.validate_output:
        validate_output()

    logging.info(ray.internal.internal_api.memory_summary(stats_only=True))


if __name__ == "__main__":
    main()
