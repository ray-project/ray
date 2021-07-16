import argparse
import csv
import itertools
import logging
import os
import random
import subprocess
import tempfile
from typing import Callable, Dict, Iterable, List

import numpy as np
import ray

from ray.experimental.raysort import constants
from ray.experimental.raysort import logging_utils
from ray.experimental.raysort import sortlib
from ray.experimental.raysort import tracing_utils
from ray.experimental.raysort.types import BlockInfo, ByteCount, RecordCount, PartId, PartInfo, Path

Args = argparse.Namespace

# ------------------------------------------------------------
#     Parse Arguments
# ------------------------------------------------------------


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray_address",
        default="auto",
        type=str,
        help="if set to None, will launch a local Ray cluster",
    )
    parser.add_argument(
        "--total_data_size",
        default=4 * 1000 * 1024 * 1024 * 1024,
        type=ByteCount,
        help="partition size in bytes",
    )
    parser.add_argument(
        "--num_mappers",
        default=1024,
        type=int,
        help="number of map tasks",
    )
    parser.add_argument(
        "--num_reducers",
        default=64,
        type=int,
        help="number of reduce actors",
    )
    parser.add_argument(
        "--merge_concurrency",
        default=8,
        type=int,
        help="number of merge tasks per node",
    )
    parser.add_argument(
        "--merge_factor",
        default=32,
        type=int,
        help="number of mapper results to buffer before merging",
    )
    parser.add_argument(
        "--reducer_chunk_size",
        default=100 * 1024 * 1024,
        type=ByteCount,
        help="number of bytes to read in memory from each mapper block",
    )
    parser.add_argument(
        "--reducer_batch_num_records",
        default=1 * 1024 * 1024,
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
        tasks_group.add_argument(f"--{task}",
                                 action="store_true",
                                 help=f"run task {task}")

    args = parser.parse_args(*args, **kwargs)
    # Derive additional arguments.
    args.input_part_size = ByteCount(args.total_data_size / args.num_mappers)
    assert args.num_mappers % args.merge_factor == 0, (args.num_mappers,
                                                       args.merge_factor)
    args.num_merged_mappers = int(args.num_mappers / args.merge_factor)
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
        return [tempfile.gettempdir()]
    return [os.path.join(mnt, d) for d in os.listdir(mnt)]


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _part_info(args: Args, part_id: PartId, kind="input") -> PartInfo:
    node = ray.worker.global_worker.node_ip_address
    mnt = random.choice(args.mount_points)
    filepath = _get_part_path(mnt, part_id, kind)
    return PartInfo(part_id, node, filepath)


def _get_part_path(mnt: Path, part_id: PartId, kind="input") -> Path:
    assert kind in {"input", "output", "temp"}
    dir_fmt = constants.DATA_DIR_FMT[kind]
    dirpath = dir_fmt.format(mnt=mnt)
    os.makedirs(dirpath, exist_ok=True)
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    filepath = os.path.join(dirpath, filename)
    return filepath


@ray.remote
def generate_part(args: Args, part_id: PartId, size: RecordCount,
                  offset: RecordCount) -> PartInfo:
    logging_utils.init()
    pinfo = _part_info(args, part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path],
        check=True)
    logging.info(f"Generated input {pinfo}")
    return pinfo


def generate_input(args: Args):
    if args.skip_input:
        return
    size = constants.bytes_to_records(args.input_part_size)
    offset = 0
    tasks = []
    for part_id in range(args.num_mappers):
        tasks.append(generate_part.remote(args, part_id, size, offset))
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


def _load_manifest(args: Args, path: Path) -> List[PartInfo]:
    if args.skip_input:
        return [PartInfo(i, None, None) for i in range(args.num_mappers)]
    with open(path) as fin:
        reader = csv.reader(fin)
        return [
            PartInfo(int(part_id), node, path)
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


@ray.remote
@tracing_utils.timeit("map")
def mapper(args: Args, boundaries: List[int], path: Path) -> List[np.ndarray]:
    logging_utils.init()

    if args.skip_input:
        part = np.frombuffer(np.random.bytes(args.input_part_size),
                             dtype=np.uint8)
    else:
        part = _load_partition(path)

    sort_fn = _dummy_sort_and_partition \
        if args.skip_sorting else sortlib.sort_and_partition
    blocks = sort_fn(part, boundaries)
    return [ray.put(part[offset:offset + size]) for offset, size in blocks]


def _dummy_merge(
        num_blocks: int, _n: int,
        get_block: Callable[[int, int], np.ndarray]) -> Iterable[memoryview]:
    blocks = [((i, 0), get_block(i, 0)) for i in range(num_blocks)]
    while len(blocks) > 0:
        (m, d), block = blocks.pop(random.randrange(len(blocks)))
        yield block
        d_ = d + 1
        block = get_block(m, d_)
        if block is None:
            continue
        blocks.append(((m, d_), block))


def _merge_impl(args: Args,
                M: int,
                pinfo: PartInfo,
                get_block: Callable[[int, int], np.ndarray],
                skip_output=False):
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, args.reducer_batch_num_records, get_block)
    if skip_output:
        for datachunk in merger:
            del datachunk
    else:
        with open(pinfo.path, "wb") as fout:
            for datachunk in merger:
                fout.write(datachunk)
    return pinfo


@ray.remote
@tracing_utils.timeit("merge")
def merge_mapper_blocks(args: Args, reducer_id: PartId, mapper_id: PartId,
                        *blocks: List[np.ndarray]) -> PartInfo:
    part_id = constants.merge_part_ids(reducer_id, mapper_id)
    pinfo = _part_info(args, part_id, kind="temp")
    blocks = ray.get(list(blocks))
    M = len(blocks)

    def get_block(i, d):
        if i >= M or d > 0:
            return None
        return blocks[i]

    _merge_impl(args, M, pinfo, get_block)
    return reducer_id, pinfo


@ray.remote
def _load_block_chunk(args: Args, pinfo: PartInfo, d: int) -> np.ndarray:
    return np.fromfile(pinfo.path,
                       dtype=np.uint8,
                       count=args.reducer_chunk_size,
                       offset=d * args.reducer_chunk_size)


@ray.remote
@tracing_utils.timeit("reduce")
def final_merge(args: Args, reducer_id: PartId,
                merged_blocks: List[PartInfo]) -> PartInfo:
    M = len(merged_blocks)
    D = ByteCount(
        np.ceil(args.total_data_size / args.num_merged_mappers /
                args.num_reducers / args.reducer_chunk_size))
    opt = _current_node_res()

    block_chunks = [
        _load_block_chunk.options(**opt).remote(args, pinfo, 0)
        for pinfo in merged_blocks
    ]

    def get_block(i, d):
        if i >= M or d >= D:
            return None
        ret = block_chunks[i]
        if d < D - 1:  # prefetch the next block chunk
            block_chunks[i] = _load_block_chunk.options(**opt).remote(
                args, merged_blocks[i], d + 1)
        return np.copy(ray.get(ret))

    pinfo = _part_info(args, reducer_id, "output")
    return _merge_impl(args, M, pinfo, get_block, args.skip_output)


def _current_node_res() -> Dict[str, float]:
    return _node_res(ray.worker.global_worker.node_ip_address)


def _node_res(node: str) -> Dict[str, float]:
    return {"resources": {f"node:{node}": 1e-3}}


@tracing_utils.timeit("sort", report_time=True)
def sort_main(args: Args):
    parts = _load_manifest(args, constants.INPUT_MANIFEST_FILE)
    boundaries = sortlib.get_boundaries(args.num_reducers)

    opt = {
        "num_returns": args.num_reducers,
        "memory": args.input_part_size * 2,
    }
    mapper_results = None
    merge_results = []
    merge_count = 0
    merge_concurrency = args.merge_concurrency * args.num_reducers

    # TODO: use placement groups to map tasks for one reducer onto
    # the same node. The current impl is not correct.
    def submit_merge_tasks():
        nonlocal merge_count
        if mapper_results is None:
            return
        if merge_count > merge_concurrency:
            ray.wait(merge_results,
                     num_returns=merge_count - merge_concurrency)
        merge_results.extend([
            merge_mapper_blocks.remote(args, r, merge_count,
                                       *mapper_results[:, r].tolist())
            for r in range(args.num_reducers)
        ])
        merge_count += args.num_reducers

    for part_id, node, path in parts:
        m = part_id % args.merge_factor
        if m == 0:
            submit_merge_tasks()
            mapper_results = np.empty((args.merge_factor, args.num_reducers),
                                      dtype=object)
        if not args.skip_input:
            opt.update(_node_res(node))
        mapper_results[m, :] = mapper.options(**opt).remote(
            args, boundaries, path)

    submit_merge_tasks()
    mapper_results = None

    merge_results = ray.get(merge_results)
    key = lambda tup: (tup[0], tup[1].node)
    reducer_inputs = [
        (k, [p for _, p in g])
        for k, g in itertools.groupby(sorted(merge_results, key=key), key=key)
    ]
    print(reducer_inputs)
    reducer_results = [
        final_merge.options(**_node_res(node)).remote(args, r, parts)
        for (r, node), parts in reducer_inputs
    ]
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
    return os.path.getsize(path)


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    partitions = _load_manifest(args, constants.OUTPUT_MANIFEST_FILE)
    results = []
    for _, node, path in partitions:
        results.append(validate_part.options(**_node_res(node)).remote(path))
    logging.info(f"Validating {len(results)} partitions")
    total = sum(ray.get(results))
    assert total == args.total_data_size, (total, args.total_data_size)
    logging.info("All OK!")


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def init(args: Args):
    if not args.ray_address:
        ray.init(resources={"worker": os.cpu_count()})
    else:
        ray.init(address=args.ray_address)
    logging_utils.init()
    logging.info(args)
    logging.info(ray.available_resources())
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    progress_tracker = tracing_utils.create_progress_tracker(args)
    return progress_tracker


def main(args: Args):
    # Keep the actor handle in scope for the duration of the program.
    _progress_tracker = init(args)

    if args.generate_input:
        generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        validate_output(args)


if __name__ == "__main__":
    main(get_args())
