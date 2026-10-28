import argparse
import asyncio
import io
import os
import uuid
from typing import Any, Dict

import numpy as np
import pandas as pd
import torch
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)
from PIL import Image
from torchvision.models import vit_b_16, ViT_B_16_Weights
import albumentations as A
import ray
import copy
import itertools
from typing import List
import string
import random
import time
from ray.data.expressions import download
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._common.network_utils import build_address
from ray._private.test_utils import NodeKillerBase, _shutdown_raylet
from ray.job_config import JobConfig

# Chaos starts once the pipeline is about to run, then waits this long so a
# backlog of produced blocks exists to lose.
CHAOS_KILL_DELAY_S = 60


WRITE_PATH = os.environ.get("RAY_DATA_WRITE_PATH") or (
    f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
)
BUCKET = "ray-benchmark-data-internal-us-west-2"

# Assumptions: homogenously shaped images, homogenous images
# Each image is 2048 * 2048 * 3 = 12.58 MB -> 11 images / block. 8 blocks per task, so ~88 images per task.
IMAGES_PER_BLOCK = 11
BLOCKS_PER_TASK = 8
NUM_UNITS = 1380
NUM_CONTAINERS = 50
OVERRIDE_NUM_BLOCKS = int(NUM_CONTAINERS * NUM_UNITS / IMAGES_PER_BLOCK)
PATCH_SIZE = 256

# Largest batch that can fit on a T4.
BATCH_SIZE = 1200

# On a T4 GPU, it takes ~11.3s to perform inference on 1200 images. So, the time per
# image is 11.3s / 1200 ~= 0.0094s.
INFERENCE_LATENCY_PER_IMAGE_S = 0.0094


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inference-concurrency",
        nargs=2,
        type=int,
        required=True,
        help="The minimum and maximum concurrency for the inference operator.",
    )
    parser.add_argument(
        "--sf",
        dest="scale_factor",
        type=int,
        default=1,
        help=(
            "The number of copies of the dataset to read. Use this to simulate a larger "
            "dataset."
        ),
    )
    parser.add_argument(
        "--chaos",
        action="store_true",
        help=(
            "Whether to enable chaos. If set, this script kills worker raylets, so "
            "the objects on those nodes are lost."
        ),
    )
    parser.add_argument(
        "--chaos-kill-interval-s",
        type=float,
        default=60,
        help="Seconds between kill rounds. Each round lands at a random point in it.",
    )
    parser.add_argument(
        "--chaos-kill-batch-size",
        type=int,
        default=2,
        help=(
            "Worker raylets killed together in each round. Above 1, several "
            "lineage reconstructions run at once."
        ),
    )
    parser.add_argument(
        "--chaos-max-kills",
        type=int,
        default=None,
        help="Stop after this many raylets are killed. Unlimited by default.",
    )
    parser.add_argument(
        "--chaos-min-alive-workers",
        type=int,
        default=12,
        help=(
            "Never kill a raylet if fewer than this many worker nodes would stay "
            "alive. Chaos pauses until replacement nodes join."
        ),
    )
    parser.add_argument(
        "--lineage-reconstruction",
        action="store_true",
        help=(
            "Start the driver with "
            "JobConfig(_disable_job_level_lineage_reconstruction=True), so Ray Data "
            "lineage reconstruction rebuilds lost objects instead of Ray Core. With "
            "--chaos, fail the run if lineage reconstruction never ran."
        ),
    )
    parser.add_argument(
        "--fake-gpu",
        action="store_true",
        help=(
            "Embed with FakeEmbedPatches, which runs on CPU nodes that advertise a "
            "logical GPU and sleeps for the measured T4 inference time."
        ),
    )
    parser.add_argument(
        "--verify-output",
        action="store_true",
        help=(
            "After the write, check the sink against the analytically-known key set "
            "(see verify_output.py) and fail the run if it does not match. Off by "
            "default so the stock benchmark path is unchanged."
        ),
    )
    return parser.parse_args()


def create_metadata(scale_factor: int):
    # TODO(mowen): Handle repeats of the dataset if scale_factor > 1
    # simulate various text metadata fields alongside image metadata
    metadata = pd.DataFrame(
        [
            {
                "metadata_0": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_1": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_2": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_3": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_4": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_5": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_6": "".join(random.choices(string.ascii_letters, k=16)),
                "container_order_read_id": f"{i:04d}_{j:04d}",
                "container_id": i,
                "channel0_uris": f"s3://{BUCKET}/15TiB-high-resolution-images/group={i:04d}/{j:04d}_{0}.png",
                "channel1_uris": f"s3://{BUCKET}/15TiB-high-resolution-images/group={i:04d}/{j:04d}_{1}.png",
                "channel2_uris": f"s3://{BUCKET}/15TiB-high-resolution-images/group={i:04d}/{j:04d}_{2}.png",
                "applied_scale": 1,
            }
            for j in range(NUM_UNITS)
            for i in range(NUM_CONTAINERS)
        ]
    )
    # Dense positional id over the frame above, so `(row_serial, patch_x, patch_y)`
    # is a primary key over the sink. Read from the source ordering rather than
    # stamped per attempt, so a re-executed task reproduces the same values --
    # a per-attempt counter would make output verification vacuous.
    metadata["row_serial"] = np.arange(len(metadata), dtype=np.int64)
    return metadata


def combine_channels(row: Dict[str, Any]) -> Dict[str, np.ndarray]:
    channels = []
    for i in range(3):
        data = io.BytesIO(row.pop(f"channel{i}"))
        image = Image.open(data)
        channels.append(np.array(image))

    row["image"] = np.dstack(channels)

    return row


def process_image(row: Dict[str, Any]) -> Dict[str, np.ndarray]:
    transform = A.Compose(
        [
            A.ToFloat(),
            A.LongestMaxSize(
                max_size=int(row["image"].shape[0] * float(1.0 / row["applied_scale"]))
            ),
            A.FromFloat(dtype="uint8"),
        ]
    )
    row["image"] = transform(image=row["image"])["image"]
    return row


def patch_image(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    image = row.pop("image")

    patches = []
    width, height, _ = image.shape
    for x, y in itertools.product(
        range(PATCH_SIZE, width - PATCH_SIZE, PATCH_SIZE),
        range(PATCH_SIZE, height - PATCH_SIZE, PATCH_SIZE),
    ):
        patch = image[y : y + PATCH_SIZE, x : x + PATCH_SIZE, :]

        patch_row = copy.deepcopy(row)
        patch_row["patch_x"] = x
        patch_row["patch_y"] = y
        patch_row["patch_width"] = PATCH_SIZE
        patch_row["patch_height"] = PATCH_SIZE
        patch_row["patch"] = patch

        patches.append(patch_row)

    return patches


class ProcessPatches:
    def __init__(self, transform):
        self._transform = transform

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["patch"] = self._transform(
            torch.as_tensor(batch["patch"]).permute(0, 3, 1, 2)
        )
        return batch


class EmbedPatches:
    def __init__(self, model, device):
        self._model = ray.get(model)
        self._model.eval()
        self._model.to(device)
        self._device = device

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.as_tensor(batch.pop("patch"), device=self._device)
        with torch.inference_mode():
            output = self._model(inputs)
            batch["embedding"] = output.cpu().numpy()
            return batch


class FakeEmbedPatches:
    def __init__(self, model, device):
        self._model = ray.get(model)
        self._model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.as_tensor(batch.pop("patch"))
        with torch.inference_mode():
            # Simulate inference latency with a sleep
            time.sleep(INFERENCE_LATENCY_PER_IMAGE_S * len(inputs))
            # Generate fake embeddings
            output = torch.rand((len(inputs), 1000), dtype=torch.float)
            batch["embedding"] = output.cpu().numpy()
            return batch


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    reconstruction_state = None
    if args.chaos and args.lineage_reconstruction:
        assert ray.data.DataContext.get_current().enable_ray_data_reconstruction, (
            "--lineage-reconstruction is set, but Ray Data lineage reconstruction is "
            "off. Check that the driver started with "
            "JobConfig(_disable_job_level_lineage_reconstruction=True)."
        )
        reconstruction_state = install_lineage_reconstruction_counter()

    print("Creating metadata")
    metadata = create_metadata(scale_factor=args.scale_factor)

    weights = ViT_B_16_Weights.DEFAULT
    model = vit_b_16(weights=weights)
    transform = weights.transforms()
    model_ref = ray.put(model)

    ds_holder = {}

    def benchmark_fn():
        ds = (
            ray.data.from_pandas(metadata)
            .with_column("channel0", download("channel0_uris"))
            .with_column("channel1", download("channel1_uris"))
            .with_column("channel2", download("channel2_uris"))
            .map(combine_channels)
            .filter(lambda row: row["image"].size != 0)
            .map(process_image)
            .flat_map(patch_image)
            .map_batches(ProcessPatches(transform), batch_size="auto")
            .map_batches(
                FakeEmbedPatches if args.fake_gpu else EmbedPatches,
                num_gpus=1,
                batch_size=BATCH_SIZE,
                concurrency=tuple(args.inference_concurrency),
                fn_constructor_kwargs={
                    "model": model_ref,
                    "device": "cpu" if args.fake_gpu else "cuda",
                },
            )
        )
        ds.write_parquet(WRITE_PATH)
        ds_holder["ds"] = ds
        if resource_killer is not None:
            # Stopped inside `benchmark_fn` because `run_fn` queries every raylet
            # for spill stats once this returns. That query fails on a node that
            # was just killed but isn't marked dead yet.
            stop_chaos(resource_killer)

    # Started here rather than at the top of `main`. The model download and
    # `ray.put` above take a while, and kills that land before the pipeline runs
    # lose nothing.
    resource_killer = start_chaos(args) if args.chaos else None

    benchmark.run_fn("main", benchmark_fn)

    metrics = collect_dataset_stats(ds_holder["ds"])
    metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
    benchmark.result["main"].update(metrics)

    if resource_killer is not None:
        nodes_killed = ray.get(resource_killer.num_killed.remote())
        print(f"Chaos killed {nodes_killed} worker raylets")
        benchmark.result["main"]["nodes_killed"] = nodes_killed

    # Recorded before `write_result` so the count actually reaches result.json.
    if reconstruction_state is not None:
        benchmark.result["main"]["lineage_reconstructions"] = reconstruction_state[
            "reconstructions"
        ]

    benchmark.write_result()

    if reconstruction_state is not None:
        # The run completing proves the dataset survived chaos; this asserts it
        # survived *because of* lineage reconstruction. Without it a run where chaos
        # never lost a needed object passes silently and verifies nothing about
        # reconstruction.
        # Asserted after `write_result` so the count is still reported on failure.
        assert reconstruction_state["reconstructions"] > 0, (
            "Chaos was enabled but Ray Data lineage reconstruction never ran, so "
            "this run did not exercise lineage reconstruction."
        )

    if args.verify_output:
        # Verified with pyarrow rather than Ray Data: checking Ray Data's output with
        # Ray Data would let a bug cancel itself out.
        from verify_output import verify

        print(f"Verifying sink {WRITE_PATH}")
        if verify(WRITE_PATH, expect_rows_missing_ok=False) != 0:
            raise RuntimeError(
                f"Output verification failed for {WRITE_PATH}; see the checks above."
            )


def install_lineage_reconstruction_counter() -> Dict[str, int]:
    """Count Ray Data lineage reconstructions for this run.

    Wraps ``LineageTracker.register_task_failed``, which the executor calls once
    per detected loss, so the run can assert lineage reconstruction actually ran
    rather than passing because chaos never lost a needed object. The streaming
    executor runs in the driver process, so patching the class here observes its
    tracker.

    Counts only calls that return seed ids. An empty list means reconstruction of
    that lineage was already under way and nothing was resubmitted.
    """
    from ray.data._internal.execution.lineage_tracker import LineageTracker

    state = {"reconstructions": 0}
    original = LineageTracker.register_task_failed

    def counting_register_task_failed(self, *args, **kwargs):
        seed_task_ids, plan_id = original(self, *args, **kwargs)
        if seed_task_ids:
            state["reconstructions"] += 1
        return seed_task_ids, plan_id

    LineageTracker.register_task_failed = counting_register_task_failed
    return state


@ray.remote(num_cpus=0)
class FlooredRayletKiller(NodeKillerBase):
    """Kills worker raylets, but keeps at least `min_alive_workers` workers alive.

    Killing the raylet takes the node's object store down with it, so the
    objects it held are lost at once. A terminator with a grace period drains the
    node first, so nothing is lost and lineage reconstruction never runs.

    `NodeKillerBase` only keeps one worker alive, so with unlimited kills a cluster
    that replaces nodes slowly would shrink to a single node. The floor bounds how
    much capacity chaos can take at once. Kills resume as replacement nodes join.
    """

    def __init__(self, *args, min_alive_workers: int, **kwargs):
        super().__init__(*args, **kwargs)
        self._min_alive_workers = min_alive_workers

    async def _find_resources_to_kill(self):
        while self.is_running:
            # A killed node can still read as alive until the GCS notices, so the
            # killed set is excluded as well.
            alive_workers = [
                (n["NodeID"], n["NodeManagerAddress"], n["NodeManagerPort"])
                for n in ray.nodes()
                if n["Alive"] and n["NodeID"] != self.head_node_id
            ]
            alive_workers = [w for w in alive_workers if w not in self.killed]
            if len(alive_workers) - self.batch_size_to_kill >= self._min_alive_workers:
                return random.sample(alive_workers, self.batch_size_to_kill)
            await asyncio.sleep(5)
        return []

    async def _kill_resource(self, node_id: str, ip: str, port: int) -> bool:
        if port is None:
            return False
        _shutdown_raylet(build_address(ip, port), graceful=False)
        return True

    def num_killed(self) -> int:
        return len(self.killed)


def stop_chaos(resource_killer, timeout_s: float = 300) -> None:
    """Stop killing raylets, then wait until the GCS marks every killed node dead.

    The GCS takes a while to notice a killed raylet. Until it does, calls that fan
    out to every alive raylet fail on that node.
    """
    ray.get(resource_killer.stop_run.remote())
    # Waits for the kill loop to exit, so a kill in flight during `stop_run` is
    # included.
    killed = {
        node_id for node_id, _, _ in ray.get(resource_killer.get_killed_nodes.remote())
    }
    deadline = time.monotonic() + timeout_s
    while True:
        still_alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]} & killed
        if not still_alive:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"{len(still_alive)} killed nodes still read as alive after "
                f"{timeout_s}s: {sorted(still_alive)}"
            )
        time.sleep(5)


def start_chaos(args: argparse.Namespace):
    assert ray.is_initialized()

    head_node_id = ray.get_runtime_context().get_node_id()
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False
    )
    resource_killer = FlooredRayletKiller.options(
        scheduling_strategy=scheduling_strategy
    ).remote(
        head_node_id,
        kill_interval_s=args.chaos_kill_interval_s,
        kill_delay_s=CHAOS_KILL_DELAY_S,
        max_to_kill=args.chaos_max_kills,
        batch_size_to_kill=args.chaos_kill_batch_size,
        min_alive_workers=args.chaos_min_alive_workers,
    )

    ray.get(resource_killer.ready.remote())

    resource_killer.run.remote()
    return resource_killer


if __name__ == "__main__":
    args = parse_args()
    init_kwargs = {}
    if args.lineage_reconstruction:
        # Ray Data lineage reconstruction is gated on this job config. It also
        # turns Ray Core lineage reconstruction off for the job.
        init_kwargs["job_config"] = JobConfig(
            _disable_job_level_lineage_reconstruction=True
        )
    ray.init(runtime_env={"py_modules": benchmark_py_modules()}, **init_kwargs)
    main(args)
