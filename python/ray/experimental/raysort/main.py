import argparse
import contextlib
import csv
import logging
import os
import random
import subprocess
import tempfile
from typing import Callable, Dict, Iterable, List

import numpy as np

import ray
from ray.experimental.raysort import constants, logging_utils, sortlib, tracing_utils
from ray.experimental.raysort.types import (
    BlockInfo,
    ByteCount,
    PartId,
    PartInfo,
    Path,
    RecordCount,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

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
        default=1 * 1000 * 1024 * 1024 * 1024,
        type=ByteCount,
        help="total data size in bytes",
    )
    parser.add_argument(
        "--num_mappers",
        default=256,
        type=int,
        help="number of map tasks",
    )
    parser.add_argument(
        "--num_mappers_per_round",
        default=16,
        type=int,
        help="number of map tasks per first-stage merge tasks",
    )
    parser.add_argument(
        "--num_reducers",
        default=16,
        type=int,
        help="number of second-stage reduce tasks",
    )
    parser.add_argument(
        "--num_concurrent_rounds",
        default=4,
        type=int,
        help="max number of rounds of map/merge tasks in flight",
    )
    parser.add_argument(
        "--reducer_input_chunk",
        default=100 * 1024 * 1024,
        type=ByteCount,
        help="bytes to read from each file in reduce tasks",
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
        "tasks to run", "if no task is specified, will run all tasks"
    )
    tasks = ["generate_input", "sort", "validate_output"]
    for task in tasks:
        tasks_group.add_argument(f"--{task}", action="store_true")

    args = parser.parse_args(*args, **kwargs)
    # Derive additional arguments.
    args.input_part_size = ByteCount(args.total_data_size / args.num_mappers)
    assert args.num_mappers % args.num_mappers_per_round == 0
    args.num_rounds = int(args.num_mappers / args.num_mappers_per_round)
    args.mount_points = _get_mount_points()
    # If no tasks are specified, run all tasks.
    args_dict = vars(args)
    if not any(args_dict[task] for task in tasks):
        for task in tasks:
            args_dict[task] = True
    return args


def _get_mount_points():
    default_ret = [tempfile.gettempdir()]
    mnt = "/mnt"
    if os.path.exists(mnt):
        ret = [os.path.join(mnt, d) for d in os.listdir(mnt)]
        if len(ret) > 0:
            return ret
    return default_ret


# ------------------------------------------------------------
#     Generate Input
# ------------------------------------------------------------


def _part_info(args: Args, part_id: PartId, kind="input") -> PartInfo:
    node = ray._private.worker.global_worker.node_ip_address
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
def generate_part(
    args: Args, part_id: PartId, size: RecordCount, offset: RecordCount
) -> PartInfo:
    logging_utils.init()
    pinfo = _part_info(args, part_id)
    subprocess.run(
        [constants.GENSORT_PATH, f"-b{offset}", f"{size}", pinfo.path], check=True
    )
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
        return [PartInfo(int(part_id), node, path) for part_id, node, path in reader]


def _load_partition(args: Args, path: Path) -> np.ndarray:
    if args.skip_input:
        return np.frombuffer(
            np.random.bytes(args.input_part_size), dtype=np.uint8
        ).copy()
    return np.fromfile(path, dtype=np.uint8)


def _dummy_sort_and_partition(
    part: np.ndarray, boundaries: List[int]
) -> List[BlockInfo]:
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
def mapper(
    args: Args, mapper_id: PartId, boundaries: List[int], path: Path
) -> List[np.ndarray]:
    logging_utils.init()
    part = _load_partition(args, path)
    sort_fn = (
        _dummy_sort_and_partition if args.skip_sorting else sortlib.sort_and_partition
    )
    blocks = sort_fn(part, boundaries)
    return [part[offset : offset + size] for offset, size in blocks]


def _dummy_merge(
    num_blocks: int, _n: int, get_block: Callable[[int, int], np.ndarray]
) -> Iterable[np.ndarray]:
    blocks = [((i, 0), get_block(i, 0)) for i in range(num_blocks)]
    while len(blocks) > 0:
        (m, d), block = blocks.pop(random.randrange(len(blocks)))
        yield block
        d_ = d + 1
        block = get_block(m, d_)
        if block is None:
            continue
        blocks.append(((m, d_), block))


def _merge_impl(
    args: Args,
    M: int,
    pinfo: PartInfo,
    get_block: Callable[[int, int], np.ndarray],
    skip_output=False,
):
    merge_fn = _dummy_merge if args.skip_sorting else sortlib.merge_partitions
    merger = merge_fn(M, get_block)

    if skip_output:
        for datachunk in merger:
            del datachunk
    else:
        with open(pinfo.path, "wb") as fout:
            for datachunk in merger:
                fout.write(datachunk)
    return pinfo


# See worker_placement_groups() for why `num_cpus=0`.
@ray.remote(num_cpus=0, resources={"worker": 1})
@tracing_utils.timeit("merge")
def merge_mapper_blocks(
    args: Args, reducer_id: PartId, mapper_id: PartId, *blocks: List[np.ndarray]
) -> PartInfo:
    part_id = constants.merge_part_ids(reducer_id, mapper_id)
    pinfo = _part_info(args, part_id, kind="temp")
    M = len(blocks)

    def get_block(i, d):
        if i >= M or d > 0:
            return None
        return blocks[i]

    return _merge_impl(args, M, pinfo, get_block)


# See worker_placement_groups() for why `num_cpus=0`.
@ray.remote(num_cpus=0, resources={"worker": 1})
@tracing_utils.timeit("reduce")
def final_merge(
    args: Args, reducer_id: PartId, *merged_parts: List[PartInfo]
) -> PartInfo:
    M = len(merged_parts)

    def _load_block_chunk(pinfo: PartInfo, d: int) -> np.ndarray:
        return np.fromfile(
            pinfo.path,
            dtype=np.uint8,
            count=args.reducer_input_chunk,
            offset=d * args.reducer_input_chunk,
        )

    def get_block(i, d):
        ret = _load_block_chunk(merged_parts[i], d)
        if ret.size == 0:
            return None
        return ret

    pinfo = _part_info(args, reducer_id, "output")
    return _merge_impl(args, M, pinfo, get_block, args.skip_output)


def _node_res(node: str) -> Dict[str, float]:
    return {"resources": {f"node:{node}": 1e-3}}


@contextlib.contextmanager
def worker_placement_groups(args: Args) -> List[ray.PlacementGroupID]:
    """
    Returns one placement group per node with a `worker` resource. To run
    tasks in the placement group, use
    `@ray.remote(num_cpus=0, resources={"worker": 1})`. Ray does not
    automatically reserve CPU resources, so tasks must specify `num_cpus=0`
    in order to run in a placement group.
    """
    pgs = [ray.util.placement_group([{"worker": 1}]) for _ in range(args.num_reducers)]
    ray.get([pg.ready() for pg in pgs])
    try:
        yield pgs
    finally:
        for pg in pgs:
            ray.util.remove_placement_group(pg)


@tracing_utils.timeit("sort", report_time=True)
def sort_main(args: Args):
    parts = _load_manifest(args, constants.INPUT_MANIFEST_FILE)
    assert len(parts) == args.num_mappers
    boundaries = sortlib.get_boundaries(args.num_reducers)

    # The exception of 'ValueError("Resource quantities >1 must be whole numbers.")'
    # will be raised if the `num_cpus` > 1 and not an integer.
    num_cpus = os.cpu_count() / args.num_concurrent_rounds
    if num_cpus > 1.0:
        num_cpus = int(num_cpus)
    mapper_opt = {
        "num_returns": args.num_reducers,
        "num_cpus": num_cpus,
    }  # Load balance across worker nodes by setting `num_cpus`.
    merge_results = np.empty((args.num_rounds, args.num_reducers), dtype=object)

    part_id = 0
    with worker_placement_groups(args) as pgs:
        for round in range(args.num_rounds):
            # Limit the number of in-flight rounds.
            num_extra_rounds = round - args.num_concurrent_rounds + 1
            if num_extra_rounds > 0:
                ray.wait(
                    [f for f in merge_results.flatten() if f is not None],
                    num_returns=num_extra_rounds * args.num_reducers,
                )

            # Submit map tasks.
            mapper_results = np.empty(
                (args.num_mappers_per_round, args.num_reducers), dtype=object
            )
            for _ in range(args.num_mappers_per_round):
                _, node, path = parts[part_id]
                m = part_id % args.num_mappers_per_round
                mapper_results[m, :] = mapper.options(**mapper_opt).remote(
                    args, part_id, boundaries, path
                )
                part_id += 1

            # Submit merge tasks.
            merge_results[round, :] = [
                merge_mapper_blocks.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pgs[r]
                    )
                ).remote(args, r, round, *mapper_results[:, r].tolist())
                for r in range(args.num_reducers)
            ]

            # Delete local references to mapper results.
            mapper_results = None

        # Submit second-stage reduce tasks.
        reducer_results = [
            final_merge.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pgs[r]
                )
            ).remote(args, r, *merge_results[:, r].tolist())
            for r in range(args.num_reducers)
        ]
        reducer_results = ray.get(reducer_results)

    if not args.skip_output:
        with open(constants.OUTPUT_MANIFEST_FILE, "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(reducer_results)

    logging.info(ray._private.internal_api.memory_summary(stats_only=True))


# ------------------------------------------------------------
#     Validate Output
# ------------------------------------------------------------


def _run_valsort(args: List[str]):
    proc = subprocess.run([constants.VALSORT_PATH] + args, capture_output=True)
    if proc.returncode != 0:
        logging.critical("\n" + proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {args}")


@ray.remote
def validate_part(path: Path):
    logging_utils.init()
    sum_path = path + ".sum"
    _run_valsort(["-o", sum_path, path])
    logging.info(f"Validated output {path}")
    with open(sum_path, "rb") as fin:
        return os.path.getsize(path), fin.read()


def validate_output(args: Args):
    if args.skip_sorting or args.skip_output:
        return
    partitions = _load_manifest(args, constants.OUTPUT_MANIFEST_FILE)
    results = []
    for _, node, path in partitions:
        results.append(validate_part.options(**_node_res(node)).remote(path))
    logging.info(f"Validating {len(results)} partitions")
    results = ray.get(results)
    total = sum(s for s, _ in results)
    assert total == args.total_data_size, total - args.total_data_size
    all_checksum = b"".join(c for _, c in results)
    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        _run_valsort(["-s", fout.name])
    logging.info("All OK!")


# ------------------------------------------------------------
#     Main
# ------------------------------------------------------------


def init(args: Args):
    if not args.ray_address:
        ray.init(resources={"worker": os.cpu_count()})
    else:
        ray.init(address=args.ray_address)
    logging.info(args)
    os.makedirs(constants.WORK_DIR, exist_ok=True)
    resources = ray.cluster_resources()
    logging.info(resources)
    args.num_workers = resources["worker"]
    progress_tracker = tracing_utils.create_progress_tracker(args)
    return progress_tracker


def main(args: Args):
    # Keep the actor handle in scope for the duration of the program.
    _progress_tracker = init(args)  # noqa F841

    if args.generate_input:
        generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        validate_output(args)


if __name__ == "__main__":
    main(get_args())
