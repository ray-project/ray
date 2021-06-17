import argparse
import csv
import logging
import os
import random
import subprocess
from typing import Iterable, List

import numpy as np
import ray

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils
from ray.experimental.raysort import sortlib
from ray.experimental.raysort import tracing_utils
from ray.experimental.raysort.types import BlockInfo, ByteCount, RecordCount, PartId, PartitionInfo, Path  # noqa: E501

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_address",
        default="auto",
        type=str,
        help="if set to None, will launch a local Ray cluster",
    )
    parser.add_argument(
        "--total_data_size",
        default=1_000_000_000,
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--num_mappers",
        default=4,
        type=int,
        help="number of map tasks",
    )
    parser.add_argument(
        "--num_reducers",
        default=4,
        type=int,
        help="number of reduce tasks",
    )
    parser.add_argument(
        "--reducer_batch_num_records",
        default=1_000_000,
        type=RecordCount,
        help="number of bytes to buffer before writing the output to EBS",
    )
    parser.add_argument(
        "--skip_sorting",
        default=False,
        action="store_true",
        help="if set, no sorting is actually performed",
    )
    parser.add_argument(
        "--skip_input",
        default=False,
        action="store_true",
        help="if set, mappers will not read data from disk",
    )
    parser.add_argument(
        "--skip_output",
        default=False,
        action="store_true",
        help="if set, reducers will not write out results to disk",
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
    args.input_part_size = ByteCount(args.total_data_size / args.num_mappers)
    args.output_part_size = ByteCount(args.total_data_size / args.num_reducers)
    args.mount_points = _get_mount_points()
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


def _get_mount_points():
    mnt = "/mnt"
    if not os.path.exists(mnt):
        return []
    return [os.path.join(mnt, d) for d in os.listdir(mnt)]


args = None

# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _make_partition_info(part_id: PartId, kind="input") -> PartitionInfo:
    node = ray.worker.global_worker.node_ip_address
    mnt = random.choice(args.mount_points)
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
    if not args.skip_input:
        subprocess.run(
            [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path],
            check=True)
        logging.info(f"Generated input {pinfo}")
    return pinfo


def generate_input():
    if args.skip_input:
        return
    size = constants.bytes_to_records(args.input_part_size)
    offset = 0
    tasks = []
    for part_id in range(args.num_mappers):
        tasks.append(generate_part.remote(part_id, size, offset))
        offset += size
    assert offset == constants.bytes_to_records(args.total_data_size), args
    logging.info(f"Generating {len(tasks)} partitions")
    parts = ray.get(tasks)
    with open(constants.INPUT_MANIFEST_FILE, "w") as fout:
        writer = csv.writer(fout)
        writer.writerows(parts)


# ------------------------------------------------------------
#     Sort
# ------------------------------------------------------------


def _load_manifest(path: Path) -> List[PartitionInfo]:
    if args.skip_input:
        return _load_dummy_manifest()
    with open(path) as fin:
        reader = csv.reader(fin)
        return [
            PartitionInfo(int(part_id), node, path)
            for part_id, node, path in reader
        ]


def _load_dummy_manifest() -> List[PartitionInfo]:
    return [PartitionInfo(i, "", "") for i in range(args.num_mappers)]


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


@ray.remote
def mapper(boundaries: List[int], mapper_id: PartId,
           path: Path) -> List[ray.ObjectRef]:
    logging_utils.init()
    task_id = f"M-{mapper_id} Mapper"
    logging.info(f"{task_id} starting {args}")
    if args.skip_input:
        block_size = int(np.ceil(args.input_part_size / args.num_reducers))
        return [
            ray.put(
                np.frombuffer(np.random.bytes(block_size), dtype=np.uint8))
            for _ in range(args.num_reducers)
        ]

    part = _load_partition(path)
    sort_fn = _dummy_sort_and_partition \
        if args.skip_sorting else sortlib.sort_and_partition
    blocks = sort_fn(part, boundaries)
    logging.info(f"{task_id} saving to object store")
    return [ray.put(part[offset:offset + size]) for offset, size in blocks]


def _dummy_merge(blocks: List[np.ndarray], _n: int) -> Iterable[memoryview]:
    for block in blocks:
        yield block


@ray.remote
def reducer(reducer_id: PartId, *blocks: List[ray.ObjectRef]) -> PartitionInfo:
    logging_utils.init()
    task_id = f"R-{reducer_id} Reducer"
    logging.info(f"{task_id} starting")
    blocks = [np.copy(ray.get(block)) for block in blocks]
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(blocks, args.reducer_batch_num_records)
    if args.skip_output:
        for datachunk in merger:
            del datachunk
        logging.info(f"{task_id} done")
        return None
    else:
        pinfo = _make_partition_info(reducer_id, "output")
        with open(pinfo.path, "wb") as fout:
            for datachunk in merger:
                fout.write(datachunk)
        logging.info(f"{task_id} done")
        return pinfo


@tracing_utils.timeit("sorting")
def sort_main():
    partitions = _load_manifest(constants.INPUT_MANIFEST_FILE)
    boundaries = sortlib.get_boundaries(args.num_reducers)
    mapper_results = np.empty(
        (args.num_mappers, args.num_reducers), dtype=object)
    for part_id, node, path in partitions:
        opt = {} if args.skip_input else {
            "resources": {
                f"node:{node}": 1 / args.num_mappers
            },
            "memory": args.input_part_size * 1.2,
        }
        opt.update(num_returns=args.num_reducers)
        mapper_results[part_id, :] = mapper.options(**opt).remote(
            boundaries, part_id, path)

    reducer_results = []
    for r in range(args.num_reducers):
        opt = {
            "memory": args.output_part_size * 1.0,
        }
        blocks = mapper_results[:, r].tolist()
        ret = reducer.options(**opt).remote(r, *blocks)
        reducer_results.append(ret)

    reducer_results = ray.get(reducer_results)
    if not args.skip_output:
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
    if args.skip_output:
        return
    partitions = _load_manifest(constants.OUTPUT_MANIFEST_FILE)
    tasks = []
    for _, node, path in partitions:
        tasks.append(
            validate_part.options(resources={
                f"node:{node}": 1 / args.num_reducers
            }).remote(path))
    logging.info(f"Validating {len(tasks)} partitions")
    ray.get(tasks)
    logging.info("All done!")


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def init():
    if args.ray_address is None:
        ray.init()
    else:
        ray.init(address=args.ray_address)
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


if __name__ == "__main__":
    args = get_args()
    main()
