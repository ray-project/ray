"""Simplified single-file ResNet50 / ImageNet-parquet ingest+training benchmark.

Same workload shape as the release test
``training_ingest_benchmark-task=image_classification.full_training.parquet``
but in one file with five knobs. Edit the file directly to change the model,
optimizer, transforms, batch size, or finalize behavior.

Reports per run: time-to-first-batch, next-batch (fetch) time, train-step
time, total runtime, throughput. With ``--num-runs > 1`` also reports
mean ± stdev across runs.

Usage:
    RAY_TRAIN_V2_ENABLED=1 python simple_ingest_benchmark.py
    RAY_TRAIN_V2_ENABLED=1 python simple_ingest_benchmark.py --num-runs=3
    RAY_TRAIN_V2_ENABLED=1 python simple_ingest_benchmark.py \\
        --num-workers=4 --limit-batches-per-worker=50 --prefetch-batches=2
"""

import argparse
import io
import os
import statistics
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import torch
import torchvision
from PIL import Image

import ray
import ray.data
import ray.train
import ray.train.torch
from ray._private.internal_api import get_state_from_address
from ray.data.collate_fn import ArrowBatchCollateFn
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# Pull ObjectStoreMemorySampler from the shared nightly-tests benchmark
# utility so we sample peak Plasma usage with the same accuracy as the
# release-test infra (1s background sampler, not just snapshots).
sys.path.insert(
    0, str(Path(__file__).resolve().parent.parent.parent / "nightly_tests" / "dataset")
)
from benchmark import (  # noqa: E402
    ObjectStoreMemorySampler,
    _get_spilled_bytes_total,
)

# Per-worker default batch count. Sized so per-step comparisons across
# variants are like-for-like regardless of batch size.
DEFAULT_BATCHES_PER_WORKER = 200

# The 1T dataset, sized so even the data_bound variant (200 × 2048 ×
# num_workers rows per run) doesn't exhaust the source.
DATA_URL = "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split_1t/train"


# --- Workload variants ------------------------------------------------------
#
# `compute_bound`: ResNet-50 at batch=32. GPU work (~300 ms / step) dominates;
# the data pipeline runs free behind it. Matches the existing release test.
#
# `data_bound`: a trivially small CNN at batch=2048. Per-step compute is ~5 ms,
# while per-batch data is ~1.2 GB and the dataloader has to decode 2048
# JPEGs. The critical path moves from GPU to "downstream of the model" —
# H2D and/or decode/collate — depending on which is slower. This regime is
# where PR1+PR2's buffer-depth and pinning choices actually affect
# throughput, not just memory.


def _make_resnet50() -> torch.nn.Module:
    return torchvision.models.resnet50(weights=None)


def _make_tiny_cnn() -> torch.nn.Module:
    return torch.nn.Sequential(
        torch.nn.Conv2d(3, 4, kernel_size=3, stride=2),
        torch.nn.AdaptiveAvgPool2d(1),
        torch.nn.Flatten(),
        torch.nn.Linear(4, 1000),
    )


VARIANTS = {
    "compute_bound": {"batch_size": 32, "make_model": _make_resnet50},
    "data_bound": {"batch_size": 8096, "make_model": _make_tiny_cnn},
}


# --- Data preprocessing -----------------------------------------------------


def _make_transform():
    return torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(224),
            torchvision.transforms.RandomHorizontalFlip(),
            torchvision.transforms.ToTensor(),
            torchvision.transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
            ),
        ]
    )


# --- Optional: pinned-CPU collate (skip Ray Data's auto GPU transfer) ------
#
# Ray Data's default path is: DefaultCollateFn -> default_finalize_fn, where
# the latter calls `move_tensors_to_device` against the train worker's GPU.
# That means the outer iterator queue holds GPU tensors and H2D happens in
# the dataloader thread before the train step gets the batch.
#
# This collate does Arrow -> pinned-CPU torch tensors and wraps them in a
# dict-like class that is NOT a `Mapping` — so `is_tensor_batch_type` returns
# False and `default_finalize_fn` is a no-op. The outer queue ends up
# holding pinned CPU tensors; the train loop does `.to(device, non_blocking=True)`
# itself, which is the canonical PyTorch pattern (pinned + non_blocking →
# async H2D on the copy engine, overlapping with the previous step's compute).


class _PinnedCpuBatch:
    """Dict-like container around pinned CPU tensors. Supports
    ``batch["image"]`` access so the train loop syntax matches the default
    codepath, but is intentionally NOT a ``Mapping`` so that
    ``is_tensor_batch_type`` returns False and Ray Data's
    ``default_finalize_fn`` does not auto-move it to GPU."""

    __slots__ = ("_tensors",)

    def __init__(self, tensors: Dict[str, torch.Tensor]) -> None:
        self._tensors = tensors

    def __getitem__(self, key: str) -> torch.Tensor:
        return self._tensors[key]


class _PinnedCpuCollate(ArrowBatchCollateFn):
    """Arrow -> pinned-CPU torch tensors. Defers H2D to the train loop."""

    def __call__(self, batch: Any) -> _PinnedCpuBatch:
        from ray.data.util.torch_utils import arrow_batch_to_tensors

        # combine_chunks=True so the resulting tensors are contiguous and
        # can be pinned in a single allocation.
        tensors = arrow_batch_to_tensors(batch, combine_chunks=True, pin_memory=True)
        return _PinnedCpuBatch(tensors)


# --- Mock loader (upper-bound throughput) ----------------------------------
#
# Yields the same pre-allocated, pre-pinned batch every iteration. Removes
# *all* dataloader cost — no S3 read, no decode, no transform, no collate,
# no Plasma round-trip, no per-batch pin. This is the strict upper bound:
# what the model + cluster could do if the data pipeline were entirely
# free. The gap between this and real-data throughput is the full cost of
# the data pipeline, including pinning.


def _mock_loader(num_batches: int, batch_size: int):
    batch = _PinnedCpuBatch(
        {
            "image": torch.randn(batch_size, 3, 224, 224).pin_memory(),
            "label": torch.randint(0, 1000, (batch_size,)).pin_memory(),
        }
    )
    for _ in range(num_batches):
        yield batch


def _make_preprocess_fn():
    # Closure so the transform is constructed once per Ray Data map task.
    transform = _make_transform()

    def preprocess(row: Dict[str, Any]) -> Dict[str, Any]:
        img = Image.open(io.BytesIO(row["image"])).convert("RGB")
        # Label in the parquet is a WNID string. We don't have the WNID→int
        # lookup here and don't need correct labels for throughput
        # benchmarking — hash to a stable [0, 1000) integer.
        label = abs(hash(row["label"])) % 1000
        return {"image": np.asarray(transform(img)), "label": label}

    return preprocess


# --- Profiler --------------------------------------------------------------
#
# Records ~10 steady-state iterations after a 10-iter warmup, dumping a
# Chrome/Perfetto-format trace per worker rank. Load the resulting JSON in
# https://ui.perfetto.dev/ to inspect H2D / kernel / CPU activity per
# iteration.
#
# Profiling adds non-trivial CPU overhead (event recording, stack walking,
# etc.). Don't use --profile runs to read throughput numbers — use them to
# inspect *what is overlapping with what* on the timeline.

TRACE_DIR = "/mnt/cluster_storage/traces"


def _make_profiler(enabled: bool, timestamp: str):
    # Only rank 0 records — extra workers' traces are mostly redundant for
    # the questions we care about (kernel timing, H2D overlap) and just add
    # file clutter and profiler overhead.
    if not enabled or ray.train.get_context().get_world_rank() != 0:
        return nullcontext()

    from torch.profiler import ProfilerActivity, profile, schedule

    os.makedirs(TRACE_DIR, exist_ok=True)
    trace_path = f"{TRACE_DIR}/simple_ingest_profile_{timestamp}.json"

    def _on_trace_ready(p) -> None:
        p.export_chrome_trace(trace_path)
        print(f"[rank 0] profiler trace saved to {trace_path}")

    return profile(
        activities=[ProfilerActivity.CPU, ProfilerActivity.CUDA],
        schedule=schedule(wait=10, warmup=5, active=10, repeat=1),
        on_trace_ready=_on_trace_ready,
    )


# --- Per-worker training loop ----------------------------------------------


def train_loop(config: Dict[str, Any]) -> Dict[str, float]:
    device = ray.train.torch.get_device()

    variant = VARIANTS[config["variant"]]
    batch_size = variant["batch_size"]

    model = variant["make_model"]()
    model = ray.train.torch.prepare_model(model)
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(
        model.parameters(), lr=0.1, momentum=0.9, weight_decay=1e-4
    )

    if config["mock"]:
        loader = _mock_loader(config["num_batches"], batch_size)
    elif config["manual_device_transfer"]:
        # Skip Ray Data's auto GPU finalize; train loop does H2D manually.
        loader = ray.train.get_dataset_shard("train").iter_torch_batches(
            batch_size=batch_size,
            prefetch_batches=config["prefetch_batches"],
            collate_fn=_PinnedCpuCollate(),
            drop_last=True,
        )
    else:
        loader = ray.train.get_dataset_shard("train").iter_torch_batches(
            batch_size=batch_size,
            prefetch_batches=config["prefetch_batches"],
            pin_memory=config["pin_memory"],
            drop_last=True,
        )

    fetch_times: List[float] = []
    # CUDA event pairs around each step. Recording an event is async — it
    # gets queued on the stream like a kernel — so it does not block the CPU
    # and does not disrupt H2D/compute pipelining. We compute elapsed times
    # only after one sync at the end of the epoch.
    step_events: List[tuple] = []

    profiler_ctx = _make_profiler(config["profile"], config["trace_timestamp"])

    model.train()
    epoch_start = time.perf_counter()
    first_batch_end = None
    last_step_end = epoch_start
    target_batches = config["num_batches"]
    batches_done = 0

    with profiler_ctx as prof:
        for batch in loader:
            if batches_done >= target_batches:
                break
            fetch_end = time.perf_counter()
            fetch_times.append(fetch_end - last_step_end)
            if first_batch_end is None:
                first_batch_end = fetch_end

            images, labels = batch["image"], batch["label"]
            if images.device != device:
                images = images.to(device, non_blocking=True)
                labels = labels.to(device, non_blocking=True)

            start_evt = torch.cuda.Event(enable_timing=True)
            end_evt = torch.cuda.Event(enable_timing=True)
            start_evt.record()
            optimizer.zero_grad(set_to_none=True)
            loss = criterion(model(images), labels)
            loss.backward()
            optimizer.step()
            end_evt.record()
            step_events.append((start_evt, end_evt))
            # CPU-side backpressure: wait for forward+loss to complete.
            # Without this, the CPU loop can run far ahead of the GPU,
            # queueing batches of work that pile up in the caching
            # allocator. `loss.item()` only waits on the loss tensor —
            # backward+optimizer continue to overlap with the next batch's
            # H2D on the CUDA copy engine. The CUDA event timings above are
            # unaffected: they record their timestamps when the GPU stream
            # processes them, independent of any CPU sync.
            loss.item()
            last_step_end = time.perf_counter()
            batches_done += 1
            if prof is not None:
                prof.step()

    # Single sync at end of epoch: ensures all queued GPU work has completed
    # before we read the events and stop the wall-clock timer.
    torch.cuda.synchronize()
    total_time = time.perf_counter() - epoch_start

    # elapsed_time returns milliseconds; convert to seconds for consistency.
    step_times = [s.elapsed_time(e) / 1000.0 for s, e in step_events]
    num_batches = len(step_times)
    total_rows = num_batches * batch_size

    def _pct(xs: List[float], p: float) -> float:
        if not xs:
            return 0.0
        return sorted(xs)[min(int(len(xs) * p), len(xs) - 1)]

    # Steady-state throughput excludes the first-batch warmup (dominated by
    # Ray Data pipeline startup, S3 reader spin-up, model/optimizer init).
    # This makes the real-data runs directly comparable to --mock, which has
    # essentially zero warmup.
    first_batch_s = (first_batch_end - epoch_start) if first_batch_end else 0.0
    steady_time = max(total_time - first_batch_s, 1e-9)

    # Steady-state next-batch time excludes fetch_times[0], which is the
    # epoch-start-to-first-batch warmup (Ray Data pipeline startup), not an
    # actual inter-step fetch.
    steady_fetch_times = fetch_times[1:]

    return {
        "total_time_s": total_time,
        "first_batch_s": first_batch_s,
        "throughput_rows_s": total_rows / total_time if total_time > 0 else 0.0,
        "steady_throughput_rows_s": total_rows / steady_time,
        "fetch_avg_ms": 1000 * sum(fetch_times) / max(len(fetch_times), 1),
        "fetch_p99_ms": 1000 * _pct(fetch_times, 0.99),
        "steady_fetch_avg_ms": (
            1000 * sum(steady_fetch_times) / max(len(steady_fetch_times), 1)
        ),
        "steady_fetch_p99_ms": 1000 * _pct(steady_fetch_times, 0.99),
        "step_avg_ms": 1000 * sum(step_times) / max(len(step_times), 1),
        "step_p99_ms": 1000 * _pct(step_times, 0.99),
        "num_batches": num_batches,
    }


# --- Driver ----------------------------------------------------------------


def run_once(args: argparse.Namespace) -> Dict[str, float]:
    num_batches_per_worker = args.limit_batches_per_worker

    # Per-run timestamp so traces from successive runs don't overwrite each
    # other. Generated on the driver so all ranks in one run share it.
    trace_timestamp = time.strftime("%Y%m%d_%H%M%S")

    train_loop_config = {
        "variant": args.variant,
        "prefetch_batches": args.prefetch_batches,
        "pin_memory": args.pin_memory,
        "manual_device_transfer": args.manual_device_transfer,
        "mock": args.mock,
        "num_batches": num_batches_per_worker,
        "profile": args.profile,
        "trace_timestamp": trace_timestamp,
    }

    datasets = None
    if not args.mock:
        # No `ds.limit()`: the train loop stops after `num_batches` via
        # `break`. `ds.limit()` would dampen upstream production and cap
        # the steady-state object-store pressure we want to measure.
        ds = ray.data.read_parquet(DATA_URL, columns=["image", "label"]).map(
            _make_preprocess_fn()
        )
        datasets = {"train": ds}

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=args.num_workers, use_gpu=True),
        datasets=datasets,
    )

    # Sample object-store usage continuously during the run so we catch
    # short-lived peaks (e.g., upstream-queue bursts before backpressure kicks in).
    state = get_state_from_address(ray.get_runtime_context().gcs_address)
    start_spilled = _get_spilled_bytes_total(state)

    with ObjectStoreMemorySampler(state, interval_s=1.0) as memory_sampler:
        result = trainer.fit().return_value

    spilled = _get_spilled_bytes_total(state) - start_spilled
    gib = 1024**3
    result["peak_object_store_gib"] = memory_sampler.peak_used_bytes / gib
    result["peak_object_store_utilization"] = memory_sampler.peak_utilization
    result["spilled_gib"] = spilled / gib
    return result


def _print_run(label: str, m: Dict[str, float]) -> None:
    print(f"\n=== {label} ===")
    print(f"  total runtime:        {m['total_time_s']:>8.2f} s")
    print(f"  time to first batch:  {m['first_batch_s']:>8.2f} s")
    print(f"  throughput (total):   {m['throughput_rows_s']:>8.1f} rows/s")
    print(f"  throughput (steady):  {m['steady_throughput_rows_s']:>8.1f} rows/s")
    print(
        f"  next-batch (total):   {m['fetch_avg_ms']:>8.3f} ms (p99 "
        f"{m['fetch_p99_ms']:.2f})"
    )
    print(
        f"  next-batch (steady):  {m['steady_fetch_avg_ms']:>8.3f} ms (p99 "
        f"{m['steady_fetch_p99_ms']:.2f})"
    )
    print(
        f"  step time:            {m['step_avg_ms']:>8.2f} ms (p99 "
        f"{m['step_p99_ms']:.2f})"
    )
    print(f"  num batches:          {m['num_batches']:>8d}")
    print(
        f"  peak object store:    {m['peak_object_store_gib']:>8.2f} GiB "
        f"({m['peak_object_store_utilization'] * 100:.1f}% utilization)"
    )
    print(f"  spilled to disk:      {m['spilled_gib']:>8.2f} GiB")


def _print_summary(runs: List[Dict[str, float]]) -> None:
    print(f"\n=== Averaged across {len(runs)} runs ===")
    keys = [
        ("total_time_s", "total runtime (s)"),
        ("first_batch_s", "time to first batch (s)"),
        ("throughput_rows_s", "throughput total (rows/s)"),
        ("steady_throughput_rows_s", "throughput steady (rows/s)"),
        ("fetch_avg_ms", "next-batch avg total (ms)"),
        ("steady_fetch_avg_ms", "next-batch avg steady (ms)"),
        ("step_avg_ms", "step avg (ms)"),
        ("peak_object_store_gib", "peak object store (GiB)"),
        ("peak_object_store_utilization", "peak object store util"),
        ("spilled_gib", "spilled to disk (GiB)"),
    ]
    for key, label in keys:
        vals = [r[key] for r in runs]
        mean = statistics.mean(vals)
        std = statistics.stdev(vals) if len(vals) > 1 else 0.0
        print(f"  {label:28s}  mean={mean:>10.3f}  std={std:>8.3f}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--variant",
        choices=sorted(VARIANTS.keys()),
        default="compute_bound",
        help=(
            "compute_bound: ResNet-50 at batch=32, GPU work dominates "
            "(~300 ms/step), data pipeline runs free behind it. "
            "data_bound: tiny CNN at batch=2048, GPU compute is ~free, "
            "critical path moves to H2D + decode/collate."
        ),
    )
    p.add_argument("--num-workers", type=int, default=16)
    p.add_argument("--num-runs", type=int, default=1)
    p.add_argument(
        "--limit-batches-per-worker",
        type=int,
        default=DEFAULT_BATCHES_PER_WORKER,
        help=(
            "Number of batches each worker iterates. Total rows = "
            "limit_batches_per_worker × batch_size × num_workers. Per-worker "
            "batch count is held fixed across variants so per-step "
            "comparisons are like-for-like."
        ),
    )
    p.add_argument("--prefetch-batches", type=int, default=4)
    p.add_argument("--pin-memory", action="store_true")
    p.add_argument(
        "--manual-device-transfer",
        action="store_true",
        help=(
            "Skip Ray Data's auto GPU finalize; produce pinned CPU tensors "
            "and have the train loop do `.to(non_blocking=True)`. "
            "Implies pin_memory=True via a custom collate, so --pin-memory "
            "is ignored when this is set."
        ),
    )
    p.add_argument(
        "--mock",
        action="store_true",
        help=(
            "Use a mock dataloader that yields the same pre-allocated pinned "
            "CPU batch every iteration. Removes all dataloader cost — gives "
            "an upper bound on training throughput for this model + cluster."
        ),
    )
    p.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Record a PyTorch profiler trace on rank 0 and save as "
            f"Chrome/Perfetto JSON to {TRACE_DIR}/"
            "simple_ingest_profile_<timestamp>.json. Each --num-runs "
            "iteration gets its own timestamp (YYYYMMDD_HHMMSS). Adds "
            "non-trivial overhead — do not use --profile runs for reading "
            "throughput numbers."
        ),
    )
    args = p.parse_args()

    batch_size = VARIANTS[args.variant]["batch_size"]
    print(
        f"config: variant={args.variant}  batch_size={batch_size}  "
        f"num_workers={args.num_workers}  "
        f"limit_batches_per_worker={args.limit_batches_per_worker}  "
        f"prefetch_batches={args.prefetch_batches}  pin_memory={args.pin_memory}  "
        f"manual_device_transfer={args.manual_device_transfer}  "
        f"mock={args.mock}  profile={args.profile}"
    )

    runs: List[Dict[str, float]] = []
    for i in range(args.num_runs):
        m = run_once(args)
        _print_run(f"Run {i + 1}/{args.num_runs}", m)
        runs.append(m)

    if args.num_runs > 1:
        _print_summary(runs)


if __name__ == "__main__":
    main()
