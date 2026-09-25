"""Weight syncing between a trainer and an inference engine using Ray Direct Transport.

This script mimics the weight synchronization step of RL for LLMs: a
"trainer" actor repeatedly updates a model and pushes the new weights to a
"generator" actor that is running a generation loop. Either actor can be killed
while the loops are running to exercise the fault tolerance path, and you can scale
each independently.

It runs the fully optimized RDT configuration:

* The sender copies each ``ray.put`` into a pre-registered NIXL memory pool
  (:func:`ray.experimental.register_nixl_memory_pool`), which buckets many small
  tensor views into contiguous, already-registered buffers.
* The receiver pre-registers its model weights with
  :func:`ray.experimental.register_nixl_memory` and points the incoming transfer
  straight at them with :func:`ray.experimental.set_target_for_ref`, so no
  staging buffer is allocated and no extra memory is registered on the data path.

Usage:

.. code-block:: bash

    python test_weights_transfer.py --device gpu

    # Split the weights into more, smaller views per transfer.
    python test_weights_transfer.py --device gpu --num-views 10000

    # Inject one generator failure.
    python test_weights_transfer.py --device gpu --max-generator-failures 1
"""

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from statistics import mean
from typing import Dict, List, Optional

import torch

import ray
from ray.actor import ActorHandle
from ray.experimental import (
    register_nixl_memory,
    register_nixl_memory_pool,
    set_target_for_ref,
)

DEFAULT_MODEL_SIZE_BYTES = 2 * 1024 * 1024 * 1024


@dataclass
class Config:
    """Knobs shared by the trainer and the generator."""

    num_views: int
    num_iters: int
    device_str: str
    model_size_bytes: int
    # Size of the sender's NIXL memory pool, or None to size it from the model.
    memory_pool_size_bytes: Optional[int]
    verify: bool

    @property
    def pool_size_bytes(self) -> int:
        if self.memory_pool_size_bytes is not None:
            return self.memory_pool_size_bytes
        # A pool block is only freed once the ObjectRef goes out of scope on
        # every generator, which can lag behind the next ray.put. Two
        # iterations' worth of headroom keeps the allocator from running dry.
        return 2 * self.model_size_bytes


class Model(torch.nn.Module):
    def __init__(self, num_views: int, total_size_bytes: int):
        super().__init__()
        # A single linear layer of `total_size_bytes`, split row-wise so that
        # each of the `num_views` views is one row.
        self.layer = torch.nn.Linear(
            total_size_bytes // num_views // 2, num_views, dtype=torch.float16
        )
        self.layer.requires_grad_(False)
        self.layer.weight.zero_()

    def forward(self, x):
        with torch.no_grad():
            x = self.layer(x)
            return x

    def get_views(self, num_views: int) -> List[torch.Tensor]:
        views = []
        for index in range(num_views):
            views.append(self.layer.weight[index])
        return views


@ray.remote(enable_tensor_transport=True)
class Generator:
    """Inference engine. Runs a generation loop and pulls weights on request."""

    def __init__(self, config: Config):
        init_start = time.perf_counter()
        self._config = config
        self._device = torch.device(config.device_str)
        self._model = Model(config.num_views, config.model_size_bytes).to(self._device)
        self._num_views = config.num_views
        self._model_version = 0
        # Since we write directly into our local model weights on every
        # transfer, register them once here instead of on each transfer.
        for param in self._model.parameters():
            register_nixl_memory(param)
        self._generation_event = asyncio.Event()
        self._timings_ms = {
            "ray_get": [],
            "Generator.__init__": [(time.perf_counter() - init_start) * 1000.0],
        }

    async def sync_weights(self, model_version: int, refs: List[ray.ObjectRef]):
        """Synchronize weights with the trainer's copy."""
        # 1. Pause generation and wait for in-flight generation to finish.
        self._generation_event.clear()
        if self._device.type == "cuda":
            torch.cuda.synchronize()

        # 2. Sync weights.
        # Unpack the ObjectRef. There should be only one, containing all tensor views.
        (ref,) = refs
        views = self._model.get_views(self._num_views)

        get_start = time.perf_counter()
        # Land the transfer directly in the model weights rather than in a
        # staging buffer we would then have to copy out of. The order of the
        # tensors here must match the order the trainer put them in.
        set_target_for_ref(ref, views)
        # The tensors this returns are the same underlying tensors as `views`,
        # so there is nothing left to copy into the model.
        ray.get(ref)
        self._timings_ms["ray_get"].append((time.perf_counter() - get_start) * 1000.0)

        if self._config.verify:
            # The trainer increments every element by one per version, so the weights should now all equal the model version.
            if not torch.all(self._model.layer.weight == model_version).item():
                raise AssertionError(
                    f"weights not synced for version {model_version}: expected "
                    f"all {model_version}, got {self._model.layer.weight[0][0].item()}"
                )
        self._model_version = model_version

        # 3. Resume generation.
        self._generation_event.set()

    async def loop(self, num_iters: int):
        """Generation loop."""
        inpt = torch.randn(
            self._model.layer.weight.shape[1], dtype=torch.float16, device=self._device
        )
        it = 0
        while True:
            # Check that generation is not paused.
            await self._generation_event.wait()

            # Simulate a generation round.
            time.sleep(0.1)
            self._model(inpt)

            # Yield, so a queued sync_weights call can run.
            await asyncio.sleep(0)
            it += 1
            if it >= num_iters:
                break

    def get_timing_metrics(self):
        return _summarize_timings(self._timings_ms)

    def get_gpu_memory_metrics(self):
        return _gpu_memory_metrics(self._device)


@ray.remote(enable_tensor_transport=True)
class Trainer:
    """Training engine. Updates weights and pushes them to the generators."""

    def __init__(self, config: Config):
        init_start = time.perf_counter()
        self._device = torch.device(config.device_str)
        self._model = Model(config.num_views, config.model_size_bytes).to(self._device)
        self._num_views = config.num_views
        self._model_version = 0
        self._generators: List[ActorHandle["Generator"]] = []
        # Pre-allocate a GPU memory pool for NIXL transfers.
        # This is instead of directly registering the Trainer's model weights.
        register_nixl_memory_pool(config.pool_size_bytes, self._device)
        self._timings_ms = {
            "ray_put": [],
            "sync_weights": [],
            "Trainer.__init__": [(time.perf_counter() - init_start) * 1000.0],
        }

    async def reset_generators(self, generators: List[ActorHandle["Generator"]]):
        """Reset the generators, possibly while the training loop is already running."""
        self._generators = generators

    async def loop(self, num_iters: int):
        """Training loop."""
        it = 0
        while True:
            # 1. Update weights.
            self._model.layer.weight += 1
            self._model_version += 1

            # 2. Put the weights in the RDT store. The weights physically stay in PyTorch memory,
            # but this registers their memory with Ray.
            views = self._model.get_views(self._num_views)
            put_start = time.perf_counter()
            weight_refs = [ray.put(views, _tensor_transport="nixl")]
            self._timings_ms["ray_put"].append(
                (time.perf_counter() - put_start) * 1000.0
            )

            # 3. Push the weights to every generator.
            sync_weights_tasks = [
                generator.sync_weights.remote(self._model_version, weight_refs)
                for generator in self._generators
            ]

            # 4. Drop our references so Ray can garbage-collect the RDT metadata
            # and free the pool blocks once the generators are done. The weights
            # stay in PyTorch memory.
            del weight_refs

            # 5. Wait for the generators to finish syncing. After this returns
            # it is safe to update the local weights again. This could instead
            # be deferred until just before the next update.
            if sync_weights_tasks:
                sync_start = time.perf_counter()
                ray.get(sync_weights_tasks)
                self._timings_ms["sync_weights"].append(
                    (time.perf_counter() - sync_start) * 1000.0
                )

            it += 1
            if it >= num_iters:
                break

            # 6. Yield to let the controller reset the list of generators if needed.
            await asyncio.sleep(0)

    def get_timing_metrics(self):
        return _summarize_timings(self._timings_ms)

    def get_bytes_per_iteration(self) -> int:
        views = self._model.get_views(self._num_views)
        return sum(view.numel() * view.element_size() for view in views)

    def get_gpu_memory_metrics(self):
        return _gpu_memory_metrics(self._device)


def _gpu_memory_metrics(device: torch.device) -> Dict[str, int]:
    if device.type != "cuda":
        return {}
    return {
        "peak_allocated_bytes": torch.cuda.max_memory_allocated(device),
        "peak_reserved_bytes": torch.cuda.max_memory_reserved(device),
    }


def _summarize_timings(timings_ms):
    summary = {}
    for name, values in timings_ms.items():
        if not values:
            summary[name] = {
                "mean_ms": None,
                "p50_ms": None,
                "p90_ms": None,
                "p100_ms": None,
                "count": 0,
            }
            continue
        sorted_vals = sorted(values)
        count = len(sorted_vals)
        p50_idx = max(0, int(0.5 * count) - 1)
        p90_idx = max(0, int(0.9 * count) - 1)
        summary[name] = {
            "mean_ms": mean(sorted_vals),
            "p50_ms": sorted_vals[p50_idx],
            "p90_ms": sorted_vals[p90_idx],
            "p100_ms": sorted_vals[-1],
            "count": count,
        }
    return summary


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-views",
        type=int,
        default=1_000,
        help="Number of tensor views the weights are split into per transfer.",
    )
    parser.add_argument("--num-iters", type=int, default=10)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="cpu")
    parser.add_argument(
        "--model-size-bytes",
        type=int,
        default=DEFAULT_MODEL_SIZE_BYTES,
        help="Total size of the model weights, transferred once per iteration.",
    )
    parser.add_argument(
        "--memory-pool-size-bytes",
        type=int,
        default=None,
        help="Size of the sender's NIXL memory pool. Defaults to twice the model size.",
    )
    parser.add_argument(
        "--no-verify",
        dest="verify",
        action="store_false",
        help="Skip checking that the received weights match the trainer's.",
    )
    parser.add_argument("--max-trainer-failures", type=int, default=0)
    parser.add_argument("--max-generator-failures", type=int, default=0)
    parser.add_argument("--kill-interval-s", type=int, default=30)
    parser.add_argument("--output-file", default=None)
    return parser.parse_args()


def main():
    args = _parse_args()

    if args.num_views > args.model_size_bytes // 2:
        raise SystemExit(
            f"--num-views {args.num_views} needs at least one float16 element per "
            f"view, but --model-size-bytes is only {args.model_size_bytes}."
        )

    config = Config(
        num_views=args.num_views,
        num_iters=args.num_iters,
        device_str="cuda" if args.device == "gpu" else "cpu",
        model_size_bytes=args.model_size_bytes,
        memory_pool_size_bytes=args.memory_pool_size_bytes,
        verify=args.verify,
    )

    ray.init()
    run_start = time.perf_counter()
    actor_opts = {"num_gpus": 1} if config.device_str == "cuda" else {}

    trainer = Trainer.options(**actor_opts).remote(config)
    generator = Generator.options(**actor_opts).remote(config)

    bytes_per_iteration = ray.get(trainer.get_bytes_per_iteration.remote())
    # Give the trainer a handle to the generator before starting the loops.
    ray.get(trainer.reset_generators.remote([generator]))
    trainer_ref = trainer.loop.remote(config.num_iters)
    generator_ref = generator.loop.remote(config.num_iters)

    trainer_failures = 0
    generator_failures = 0
    num_trainer_kills = 0
    num_generator_kills = 0
    last_kill_time = time.time()

    # Inject failures until we have hit the requested budget for both actors.
    while (
        trainer_failures < args.max_trainer_failures
        or generator_failures < args.max_generator_failures
    ):
        # A ref becomes ready if its actor process dies or if the loop exits.
        failed, _ = ray.wait(
            [trainer_ref, generator_ref], num_returns=1, timeout=args.kill_interval_s
        )

        if time.time() - last_kill_time > args.kill_interval_s:
            if num_trainer_kills < args.max_trainer_failures:
                print("killing trainer")
                ray.kill(trainer, force=True)
                num_trainer_kills += 1
            if num_generator_kills < args.max_generator_failures:
                print("killing generator")
                ray.kill(generator, force=True)
                num_generator_kills += 1
            last_kill_time = time.time()

        if not failed:
            # ray.wait timed out; nothing has finished or died yet.
            continue

        if failed[0] is trainer_ref:
            try:
                ray.get(trainer_ref)
                break
            except Exception as e:
                print("trainer failed", e)
                trainer_failures += 1
            print("starting new trainer")
            trainer = Trainer.options(**actor_opts).remote(config)
            ray.get(trainer.reset_generators.remote([generator]))
            trainer_ref = trainer.loop.remote(config.num_iters)
        else:
            try:
                ray.get(generator_ref)
                break
            except Exception as e:
                print("generator failed", e)
                generator_failures += 1
            print("starting new generator")
            generator = Generator.options(**actor_opts).remote(config)
            trainer.reset_generators.remote([generator])
            generator_ref = generator.loop.remote(config.num_iters)

    ray.get([trainer_ref, generator_ref])
    run_time_ms = (time.perf_counter() - run_start) * 1000.0
    print("job complete")

    output = {
        "num_views": config.num_views,
        "num_iters": config.num_iters,
        "num_bytes_per_iter": bytes_per_iteration,
        "device": args.device,
        "memory_pool_size_bytes": config.pool_size_bytes,
        "max_trainer_failures": args.max_trainer_failures,
        "max_generator_failures": args.max_generator_failures,
        "num_trainer_kills": num_trainer_kills,
        "num_generator_kills": num_generator_kills,
        "total_run_time_ms": run_time_ms,
        "trainer_metrics": ray.get(trainer.get_timing_metrics.remote()),
        "generator_metrics": ray.get(generator.get_timing_metrics.remote()),
        "trainer_gpu_memory": ray.get(trainer.get_gpu_memory_metrics.remote()),
        "generator_gpu_memory": ray.get(generator.get_gpu_memory_metrics.remote()),
    }
    print(json.dumps(output, indent=2))
    if args.output_file:
        with open(args.output_file, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(output) + "\n")


if __name__ == "__main__":
    main()
