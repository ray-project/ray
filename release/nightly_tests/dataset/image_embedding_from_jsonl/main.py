# ABOUTME: Ray Data image embedding benchmark (JSONL input) with GPU and CPU profiling.
# ABOUTME: Reads base64-encoded images from JSONL, runs HuggingFace ViT inference on GPU actors, writes to parquet.

from __future__ import annotations

import argparse
import os
import time
import uuid
from io import BytesIO
from typing import Any, Dict, List

import numpy as np
import ray
import ray.data
import torch
from transformers import ViTImageProcessor, ViTForImageClassification
from PIL import Image
from pybase64 import b64decode

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._private.test_utils import EC2InstanceTerminatorWithGracePeriod
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)
from profiling.coordinator import Profiling
from profiling import nvtx as profiling_nvtx
from profiling.metrics import extract_pipeline_metrics


INPUT_PREFIX = "s3://ray-benchmark-data-internal-us-west-2/10TiB-jsonl-images"
OUTPUT_PREFIX = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

BATCH_SIZE = 1024

PROCESSOR = ViTImageProcessor(
    do_convert_rgb=None,
    do_normalize=True,
    do_rescale=True,
    do_resize=True,
    image_mean=[0.5, 0.5, 0.5],
    image_std=[0.5, 0.5, 0.5],
    resample=2,
    rescale_factor=0.00392156862745098,
    size={"height": 224, "width": 224},
)

JOB_ID = os.environ.get("ANYSCALE_JOB_ID", f"local-{uuid.uuid4().hex[:8]}")
SHARED_OUTDIR = f"/mnt/shared_storage/image_embedding_jsonl/{JOB_ID}"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inference-concurrency",
        nargs=2,
        type=int,
        required=True,
        help="The minimum and maximum concurrency for the inference operator.",
    )
    parser.add_argument(
        "--chaos",
        action="store_true",
        help=(
            "Whether to enable chaos. If set, this script terminates one worker node "
            "every minute with a grace period."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Pipeline UDFs
# ---------------------------------------------------------------------------


def decode(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    image_data = b64decode(row["image"], None, True)
    image = Image.open(BytesIO(image_data))
    width, height = image.size
    return [
        {
            "original_url": row["url"],
            "original_width": width,
            "original_height": height,
            "image": np.asarray(image),
        }
    ]


def preprocess(row: Dict[str, Any]) -> Dict[str, Any]:
    outputs = PROCESSOR(images=row["image"])["pixel_values"]
    assert len(outputs) == 1, len(outputs)
    row["image"] = outputs[0]
    return row


class Infer:
    def __init__(self):
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model = ViTForImageClassification.from_pretrained(
            "google/vit-base-patch16-224"
        ).to(self._device)

        self._call_count = 0
        self._profiling_active = False
        self._profiler_done = False
        self._profiler_mode = os.environ.get("PROFILER_MODE", "none")
        self._skip_batches = int(os.environ.get("PROFILE_SKIP_BATCHES", "0"))
        self._active_batches = int(os.environ.get("PROFILE_ACTIVE_BATCHES", "10000"))
        self._node_ip = ray.util.get_node_ip_address()

        # The capture range opens at the first cuda_profiler_fence call
        # (batch == skip_batches + 1) and normally never closes via the
        # in-loop fence (active_batches is set high). Atexit closes it on
        # interpreter shutdown; with capture-range-end:stop in
        # nsys_runtime_env() that finalizes the .nsys-rep synchronously
        # before Ray tears the actor down.
        if self._profiler_mode == "nsys":
            import atexit

            def _stop_nsys():
                if self._profiling_active:
                    torch.cuda.cudart().cudaProfilerStop()
                    self._profiling_active = False

            atexit.register(_stop_nsys)

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        self._call_count += 1

        # --- nsys capture range control via CUDA profiler API ---
        if self._profiler_mode == "nsys" and not self._profiler_done:
            result = profiling_nvtx.cuda_profiler_fence(
                self._call_count,
                self._skip_batches,
                self._active_batches,
                self._node_ip,
            )
            if result[0] is not None:
                self._profiling_active = result[0]
            if result[1] is not None:
                self._profiler_done = result[1]

        # --- GPU work (with NVTX annotations when nsys is active) ---
        if self._profiler_mode == "nsys":
            with profiling_nvtx.profiling_range(f"MapBatches_call_{self._call_count}"):
                with profiling_nvtx.profiling_range("h2d_transfer"):
                    next_tensor = torch.from_numpy(batch["image"]).to(
                        dtype=torch.float32,
                        device=self._device,
                        non_blocking=True,
                    )

                with profiling_nvtx.profiling_range("inference"):
                    with torch.inference_mode():
                        output = self._model(next_tensor).logits

                with profiling_nvtx.profiling_range("d2h_postprocess"):
                    result = {
                        "original_url": batch["original_url"],
                        "original_width": batch["original_width"],
                        "original_height": batch["original_height"],
                        "output": output.cpu().numpy(),
                    }
        else:
            next_tensor = torch.from_numpy(batch["image"]).to(
                dtype=torch.float32, device=self._device, non_blocking=True
            )
            with torch.inference_mode():
                output = self._model(next_tensor).logits
            result = {
                "original_url": batch["original_url"],
                "original_width": batch["original_width"],
                "original_height": batch["original_height"],
                "output": output.cpu().numpy(),
            }

        return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(args: argparse.Namespace, profiling: Profiling):
    benchmark = Benchmark()

    if args.chaos:
        start_chaos()

    infer_kwargs = {
        "batch_size": BATCH_SIZE,
        "num_gpus": 1,
        "concurrency": tuple(args.inference_concurrency),
    }

    nsys_env = profiling.nsys_runtime_env()
    if nsys_env:
        infer_kwargs["runtime_env"] = nsys_env

    num_gpus = max(args.inference_concurrency)

    def benchmark_fn():
        ds = (
            ray.data.read_json(INPUT_PREFIX, lines=True)
            .flat_map(decode)
            .map(preprocess)
            .map_batches(
                Infer,
                **infer_kwargs,
            )
        )
        ds.write_parquet(OUTPUT_PREFIX)
        metrics = collect_dataset_stats(ds)
        metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
        # Hold ds in scope so Ray Data keeps the actor pool alive while nsys
        # finalizes its .nsys-rep files via stop-on-exit / atexit. Without
        # this, ds drops out of scope on return and Ray tears the actors
        # down before nsys gets to flush.
        if profiling.profiler_mode == "nsys":
            print("Holding ds in scope for 30s to let nsys finalize...", flush=True)
            time.sleep(30)
        if profiling.is_enabled():
            metrics.update(
                extract_pipeline_metrics(ds, num_gpus=num_gpus, outdir=SHARED_OUTDIR)
            )
        return metrics

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()

    # Copy result.json to shared storage for telemetry upload.
    import shutil

    result_path = os.environ.get("TEST_OUTPUT_JSON", "./result.json")
    if os.path.exists(result_path):
        shutil.copy2(result_path, SHARED_OUTDIR)


def start_chaos():
    assert ray.is_initialized()

    head_node_id = ray.get_runtime_context().get_node_id()
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False
    )
    resource_killer = EC2InstanceTerminatorWithGracePeriod.options(
        scheduling_strategy=scheduling_strategy
    ).remote(head_node_id, max_to_kill=None)

    ray.get(resource_killer.ready.remote())

    resource_killer.run.remote()


if __name__ == "__main__":
    ray.init(runtime_env={"py_modules": benchmark_py_modules()})
    args = parse_args()

    # S3 sometimes returns transient ACCESS_DENIED on HeadObject under heavy
    # concurrent load (credential refresh or throttling). Retry these instead
    # of aborting the entire job.
    ctx = ray.data.DataContext.get_current()
    ctx.retried_io_errors = list(ctx.retried_io_errors) + [
        "AWS Error ACCESS_DENIED",
    ]

    num_gpu_nodes = max(args.inference_concurrency)
    profiling = Profiling(outdir=SHARED_OUTDIR, num_gpu_nodes=num_gpu_nodes)

    profiling.start(
        extra_config={
            "RAY_COMMIT": ray.__commit__,
            "INFERENCE_CONCURRENCY": args.inference_concurrency,
        }
    )
    try:
        main(args, profiling)
    finally:
        profiling.stop(s3_prefix=f"image-embedding-jsonl/{JOB_ID}")
