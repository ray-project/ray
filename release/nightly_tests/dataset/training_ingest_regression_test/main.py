"""Single-file Ray Data + Ray Train ingest benchmark for iter-batches.

Probes the iter-batches consumer pipeline under conditions that exercise
both consumer-side object store usage and pipeline throughput. Reports per run:
time-to-first-batch, next-batch (fetch) time, train-step time, total
runtime, throughput, peak object store usage. With ``--num-runs > 1``
also reports mean ± stdev across runs.

Release-test configurations:
- ``peak_object_store_memory``: big batch + slow consumer (``--step-sleep-s=2.0``
  simulates a big-model training step dominated by ND-parallel collectives).
  Buffers fill under back-pressure so peak consumer-side object-store
  usage is the dominant signal. Cannot detect pipeline-throughput
  regressions (sleep dominates step time).
- ``throughput``: same config without the sleep. Pipeline is the
  rate-limiter so a pipeline-rate regression shows up. Cannot see the
  object store usage signal (consumer-side queues stay empty when consumer is fast).
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
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# Pull ObjectStoreMemorySampler from the sibling benchmark utility so we
# sample peak Plasma usage with the same accuracy as the release-test
# infra (1s background sampler, not just snapshots).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark import (  # noqa: E402
    ObjectStoreMemorySampler,
    _get_spilled_bytes_total,
)

# Per-worker default batch count.
DEFAULT_BATCHES_PER_WORKER = 200

# The 1T dataset, sized so a long run
# (DEFAULT_BATCHES_PER_WORKER × BATCH_SIZE × num_workers rows per run)
# doesn't exhaust the source.
DATA_URL = "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split_1t/train"


# --- Workload ---------------------------------------------------------------
#
# A trivially small CNN at batch=1024. Per-step compute is ~5 ms while
# per-batch data is ~588 MiB and the dataloader has to decode 1024
# JPEGs, so the critical path is downstream of the model — H2D and/or
# decode/collate. This is the regime where iter-batches buffer depth
# and pinning choices affect throughput AND peak object-store usage.
# Use `--step-sleep-s=2.0` to back-pressure the pipeline and exercise
# the peak-memory case; `--step-sleep-s=0` for the throughput case.

BATCH_SIZE = 1024


def _make_model() -> torch.nn.Module:
    return torch.nn.Sequential(
        torch.nn.Conv2d(3, 4, kernel_size=3, stride=2),
        torch.nn.AdaptiveAvgPool2d(1),
        torch.nn.Flatten(),
        torch.nn.Linear(4, 1000),
    )


class _SleepingModel(torch.nn.Module):
    """Wraps a base model and sleeps for ``sleep_s`` seconds inside
    ``forward`` after running the wrapped module. Used to simulate a
    slower model (e.g. a much larger model, or one whose forward/backward
    is dominated by ND-parallel collective communication) without
    actually consuming the GPU memory a real large model would need.

    The sleep extends per-step wall-clock from the dataloader's
    perspective, building back-pressure in the iter-batches queues — the
    regime where consumer-side buffer size shows up in peak object-store
    usage.
    """

    def __init__(self, base: torch.nn.Module, sleep_s: float) -> None:
        super().__init__()
        self.base = base
        self.sleep_s = sleep_s

    def forward(self, *args: Any, **kwargs: Any) -> Any:
        out = self.base(*args, **kwargs)
        if self.sleep_s > 0:
            time.sleep(self.sleep_s)
        return out


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


# --- Mock loader (upper-bound throughput) ----------------------------------
#
# Yields the same pre-allocated, pre-pinned batch every iteration. Removes
# *all* dataloader cost — no S3 read, no decode, no transform, no collate,
# no Plasma round-trip, no per-batch pin. This is the strict upper bound:
# what the model + cluster could do if the data pipeline were entirely
# free. The gap between this and real-data throughput is the full cost of
# the data pipeline, including pinning.


def _mock_loader(num_batches: int, batch_size: int):
    batch = {
        "image": torch.randn(batch_size, 3, 224, 224).pin_memory(),
        "label": torch.randint(0, 1000, (batch_size,)).pin_memory(),
    }
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
    trace_path = f"{TRACE_DIR}/training_ingest_profile_{timestamp}.json"

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

    model = _make_model()
    if config["step_sleep_s"] > 0:
        model = _SleepingModel(model, config["step_sleep_s"])
    model = ray.train.torch.prepare_model(model)
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(
        model.parameters(), lr=0.1, momentum=0.9, weight_decay=1e-4
    )

    if config["mock"]:
        loader = _mock_loader(config["num_batches"], BATCH_SIZE)
    else:
        loader = ray.train.get_dataset_shard("train").iter_torch_batches(
            batch_size=BATCH_SIZE,
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
    total_rows = num_batches * BATCH_SIZE

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
        "prefetch_batches": args.prefetch_batches,
        "pin_memory": args.pin_memory,
        "mock": args.mock,
        "num_batches": num_batches_per_worker,
        "profile": args.profile,
        "trace_timestamp": trace_timestamp,
        "step_sleep_s": args.step_sleep_s,
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
    p.add_argument("--num-workers", type=int, default=16)
    p.add_argument("--num-runs", type=int, default=1)
    p.add_argument(
        "--limit-batches-per-worker",
        type=int,
        default=DEFAULT_BATCHES_PER_WORKER,
        help=(
            "Number of batches each worker iterates. Total rows = "
            "limit_batches_per_worker × batch_size × num_workers."
        ),
    )
    p.add_argument("--prefetch-batches", type=int, default=4)
    p.add_argument("--pin-memory", action="store_true")
    p.add_argument(
        "--step-sleep-s",
        type=float,
        default=0.0,
        help=(
            "Sleep this many seconds inside model.forward() to simulate a "
            "larger model (e.g. one whose forward/backward is dominated by "
            "ND-parallel collectives) without actually consuming GPU memory. "
            "Adds wall-clock to each step so back-pressure builds up in the "
            "dataloader queues — the regime where consumer-side buffer size "
            "shows up in peak object-store usage."
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
            "training_ingest_profile_<timestamp>.json. Each --num-runs "
            "iteration gets its own timestamp (YYYYMMDD_HHMMSS). Adds "
            "non-trivial overhead — do not use --profile runs for reading "
            "throughput numbers."
        ),
    )
    args = p.parse_args()

    print(
        f"config: batch_size={BATCH_SIZE}  "
        f"num_workers={args.num_workers}  "
        f"limit_batches_per_worker={args.limit_batches_per_worker}  "
        f"prefetch_batches={args.prefetch_batches}  pin_memory={args.pin_memory}  "
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
