"""Scaling benchmark for ``ray.data.read_lerobot``.

This mirrors the other Ray Data *reading* benchmarks in this directory
(``read_and_consume_benchmark.py``, ``read_from_uris_benchmark.py``): it reads a
real public dataset off cloud storage, fully consumes it, and emits a JSON perf
file via the shared :class:`benchmark.Benchmark` harness so the release-test
infra can ingest it.

Unlike the file-format readers, ``read_lerobot`` decodes per-episode MP4 camera
streams (via torchcodec) or in-parquet images, so it is CPU-bound on video
decode rather than I/O-bound. The interesting scaling knob is therefore
``override_num_blocks`` -- how finely the per-video-file read tasks are split
across the cluster's CPUs. We sweep it (e.g. 16/48/96) and, for each setting,
fully consume the dataset and record throughput + memory.

To keep the object store bounded while still forcing a *full* decode of every
frame (the expensive part), we map each decoded camera frame down to a single
scalar (its mean brightness) right after the read, exactly like a real
downstream featurization step would shrink the per-row payload. This is what
keeps peak object-store usage at a few MB even though ~100k HWC uint8 frames are
decoded -- the decode work is real, only the materialized output is small.

Example (the configuration the released scaling job uses)::

    # libero_10 over the HuggingFace hub filesystem (hffs), 3-node cluster:
    python lerobot_read_benchmark.py \
        --root hf://datasets/lerobot/libero_10 --use-hffs --blocks 16 48 96

    # libero-mini smoke test over anonymous S3 (843 frames, single node ok):
    python lerobot_read_benchmark.py \
        --root s3://anonymous@ray-example-data/lerobot/libero-mini --blocks 8

The emitted JSON (consumed by the release dashboard) has one entry per
``override_num_blocks`` case, each carrying: wall time, row/frame throughput,
per-task peak heap (MiB, parsed from ``ds.stats()``), peak object-store usage
(GB, sampled by the harness), spilled bytes, number of nodes used, and the rows
processed per node (a distribution check that the read actually fanned out
across the cluster).
"""

import argparse
import json
import re
from collections import Counter
from typing import Any, Dict, List, Optional

import numpy as np

import ray
from benchmark import Benchmark, BenchmarkMetric


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=str,
        default="s3://anonymous@ray-example-data/lerobot/libero-mini",
        help=(
            "Dataset root URI passed to ray.data.read_lerobot. Local, s3://, "
            "gs://, or hf:// (with --use-hffs). Defaults to the libero-mini "
            "smoke dataset on anonymous S3."
        ),
    )
    parser.add_argument(
        "--blocks",
        type=int,
        nargs="+",
        default=[16, 48, 96],
        help=(
            "override_num_blocks values to sweep. Each is run as a separate "
            "benchmark case against a freshly-created dataset."
        ),
    )
    parser.add_argument(
        "--use-hffs",
        action="store_true",
        help=(
            "Read through the HuggingFace hub filesystem (huggingface_hub's "
            "fsspec implementation). Required for hf://datasets/... roots."
        ),
    )
    parser.add_argument(
        "--group-by-episode",
        action="store_true",
        help="Pass group_by_episode=True (one read task per episode).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="iter_batches batch size used while consuming (default: Ray's).",
    )
    return parser.parse_args()


def _make_filesystem(use_hffs: bool):
    """Build the pyarrow/fsspec filesystem for the read, if any.

    For hf:// roots we hand read_lerobot an explicit HfFileSystem so both the
    metadata/parquet reads and the by-URI video decode resolve against the hub.
    For s3://anonymous@... and local roots we let read_lerobot pick the
    filesystem from the URI scheme.
    """
    if not use_hffs:
        return None
    from huggingface_hub import HfFileSystem

    return HfFileSystem()


def _tensor_arrow_types() -> tuple:
    """Tuple of the Arrow tensor extension types Ray Data uses for ndarray
    columns. Decoded camera frames land in one of these (variable-shaped for
    HWC uint8 video/image frames); scalars/1-D vectors do not."""
    from ray.data.extensions import (
        ArrowTensorType,
        ArrowTensorTypeV2,
        ArrowVariableShapedTensorType,
    )

    return (ArrowTensorType, ArrowTensorTypeV2, ArrowVariableShapedTensorType)


def _camera_columns(schema) -> List[str]:
    """Return the decoded-camera columns (HWC uint8 tensors) from a Dataset
    schema. These are the columns produced by decoding video_keys/image_keys;
    everything else (index, episode_index, state, action, task, ...) is scalar
    or 1-D. We detect them as the tensor-typed columns whose decoded frames are
    multi-dimensional (HWC), which excludes 1-D state/action vectors."""
    tensor_types = _tensor_arrow_types()
    camera_cols = []
    for name, dtype in zip(schema.names, schema.types):
        if not isinstance(dtype, tensor_types):
            continue
        # Camera frames are HWC (ndim >= 2 before the implicit batch axis);
        # state/action are 1-D tensors. Variable-shaped types don't expose a
        # static shape, so treat them as cameras (frames are the only
        # variable-shaped column read_lerobot emits).
        shape = getattr(dtype, "shape", None)
        if shape is None or len(shape) >= 2:
            camera_cols.append(name)
    return camera_cols


def _make_reduce_decode(camera_cols: List[str]):
    """Build a map_batches fn that forces a full camera decode but emits only a
    tiny scalar per frame, keeping the materialized blocks small.

    We compute each frame's mean brightness. Reading ``batch[col]`` materializes
    the decoded HWC uint8 frames (the expensive torchcodec/Pillow work happened
    in the read task; this guarantees we actually touch every pixel), then we
    drop the heavy tensor columns from the output."""

    def reduce_decode(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        n = len(next(iter(batch.values())))
        brightness = np.zeros(n, dtype=np.float64)
        for col in camera_cols:
            frames = batch[col]
            for i in range(n):
                # Touch every pixel of every decoded frame.
                brightness[i] += float(np.asarray(frames[i]).mean())
        out = {k: v for k, v in batch.items() if k not in camera_cols}
        out["mean_brightness"] = brightness
        return out

    return reduce_decode


def _parse_peak_heap_mib(stats: str) -> Optional[float]:
    """Parse the max 'Peak heap memory usage (MiB)' across operators from
    ds.stats(). This is the per-task heap high-water mark Ray Data reports."""
    peaks = [
        float(m)
        for m in re.findall(r"Peak heap memory usage \(MiB\):.*?([\d.]+) max", stats)
    ]
    return max(peaks) if peaks else None


def _read_lerobot_case(
    args: argparse.Namespace,
    num_blocks: int,
    camera_cols_holder: Dict[str, Any],
) -> Dict[str, Any]:
    """Run a single override_num_blocks case end-to-end and return its metrics.

    Returns a dict of extra metrics; the Benchmark harness adds wall time,
    object-store peak/utilization, and spilled bytes around this call.
    """
    filesystem = _make_filesystem(args.use_hffs)
    ds = ray.data.read_lerobot(
        args.root,
        filesystem=filesystem,
        read_granularity="episode" if args.group_by_episode else "file",
        override_num_blocks=num_blocks,
    )

    # Discover the decoded-camera columns once (schema triggers a metadata-only
    # read, not a full scan). Cache across cases since the schema is stable.
    camera_cols = camera_cols_holder.get("cols")
    if camera_cols is None:
        camera_cols = _camera_columns(ds.schema())
        camera_cols_holder["cols"] = camera_cols
    print(f"[case blocks={num_blocks}] camera columns: {camera_cols}")

    if camera_cols:
        ds = ds.map_batches(_make_reduce_decode(camera_cols))

    # Fully consume by materializing. Every row is decoded, but because the
    # reduce step dropped the heavy camera tensors the materialized blocks are
    # only a few MB total -- small enough to hold and to introspect per-node
    # placement from. (materialize() runs the whole pipeline to completion.)
    mds = ds.materialize()

    num_rows = mds.count()
    num_batches = 0
    iter_kwargs = {}
    if args.batch_size is not None:
        iter_kwargs["batch_size"] = args.batch_size
    for _ in mds.iter_batches(**iter_kwargs):
        num_batches += 1

    node_rows = _rows_per_node(mds)

    stats = mds.stats()
    nodes_used = _parse_nodes_used(stats) or len(node_rows) or None
    peak_heap_mib = _parse_peak_heap_mib(stats)

    num_cameras = len(camera_cols)
    frame_decodes = num_rows * num_cameras

    return {
        "override_num_blocks": num_blocks,
        BenchmarkMetric.NUM_ROWS.value: num_rows,
        "cameras": num_cameras,
        "num_batches": num_batches,
        "frame_decodes": frame_decodes,
        "nodes_used": nodes_used,
        "peak_heap_mib": peak_heap_mib,
        "rows_per_node": node_rows,
        "stats_excerpt": _stats_excerpt(stats),
    }


def _rows_per_node(mds) -> Dict[str, int]:
    """Map each materialized block to the node that holds it and sum rows, so we
    can confirm the read fanned out across the cluster (not all on the head)."""
    try:
        bundles = list(mds.iter_internal_ref_bundles())
    except Exception:
        return {}

    block_refs = []
    rows_by_ref: Dict[Any, int] = {}
    for bundle in bundles:
        for entry in bundle.blocks:
            # Some ray versions yield (ref, metadata) tuples here; others yield
            # objects with .ref/.metadata. Tolerate both.
            if isinstance(entry, tuple):
                ref, meta = entry[0], entry[1]
            else:
                ref, meta = entry.ref, entry.metadata
            block_refs.append(ref)
            rows_by_ref[ref] = getattr(meta, "num_rows", None) or 0

    try:
        locations = ray.experimental.get_object_locations(block_refs)
    except Exception:
        return {}

    rows_by_node: Counter = Counter()
    for block_ref in block_refs:
        node_ids = locations.get(block_ref, {}).get("node_ids", [])
        node_id = node_ids[0][:8] if node_ids else "unknown"
        rows_by_node[node_id] += rows_by_ref.get(block_ref, 0)
    return dict(rows_by_node)


def _parse_nodes_used(stats: str) -> Optional[int]:
    """Parse 'N nodes used' from ds.stats()."""
    matches = [int(m) for m in re.findall(r"(\d+) nodes used", stats)]
    return max(matches) if matches else None


def _stats_excerpt(stats: str, max_chars: int = 2000) -> str:
    """Trim ds.stats() to the operator block for the JSON output."""
    return stats[:max_chars]


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()
    cluster_cpus = ray.cluster_resources().get("CPU", 0)
    print(f"Cluster CPUs: {cluster_cpus}; root: {args.root}")

    # Shared holder so the camera-column schema is computed once.
    camera_cols_holder: Dict[str, Any] = {}

    for num_blocks in args.blocks:
        case_name = f"override_num_blocks={num_blocks}"

        def case_fn(nb=num_blocks):
            # run_fn wraps this in its wall-time + object-store-memory timers and
            # merges the returned dict into the case's metrics. Derived
            # throughputs (rows/s, frames/s) are computed below from the
            # harness-recorded wall time, once it's available.
            return _read_lerobot_case(args, nb, camera_cols_holder)

        benchmark.run_fn(case_name, case_fn)

        # Augment the just-recorded case with derived throughput using the
        # harness-recorded wall time (BenchmarkMetric.RUNTIME).
        result = benchmark.result[case_name]
        wall_s = result.get(BenchmarkMetric.RUNTIME.value)
        rows = result.get(BenchmarkMetric.NUM_ROWS.value, 0)
        frames = result.get("frame_decodes", 0)
        if wall_s and wall_s > 0:
            result[BenchmarkMetric.THROUGHPUT.value] = round(rows / wall_s, 2)
            result["rows_per_s"] = round(rows / wall_s, 2)
            result["frame_decodes_per_s"] = round(frames / wall_s, 2)
        result["cluster_cpus"] = cluster_cpus
        print(f"[case {case_name}] {json.dumps(_loggable(result), indent=2)}")

    benchmark.write_result()


def _loggable(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop the long stats_excerpt for concise console logging."""
    return {k: v for k, v in d.items() if k != "stats_excerpt"}


if __name__ == "__main__":
    ray.init()
    main(parse_args())
