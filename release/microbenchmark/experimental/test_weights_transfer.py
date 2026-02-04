import argparse
import asyncio
import time
from statistics import mean
from typing import List

import torch

import ray
from ray.actor import ActorHandle

# TODOs:
# - test failover
# - add performance metrics
# - test with NIXL


class Model(torch.nn.Module):
    def __init__(self, num_indices: int):
        super().__init__()
        # 2GiB matrix.
        TOTAL_SIZE_BYTES = 2 * 1024 * 1024 * 1024
        NUM_ROWS = 10_000
        self.layer = torch.nn.Linear(
            NUM_ROWS, TOTAL_SIZE_BYTES // NUM_ROWS // 2, dtype=torch.float16
        )
        self.layer.requires_grad_(False)
        self.layer.weight.zero_()
        self.indices = list(range(num_indices))

    def forward(self, x):
        x = self.layer(x)
        return x

    def get_views(self):
        views = []
        for index in self.indices:
            views.append(self.layer.weight[index])
        return views


@ray.remote(enable_tensor_transport=True)
class Generator:
    def __init__(self, num_indices: int, device_str: str):
        self._device = torch.device(device_str)
        self._model = Model(num_indices).to(self._device)
        self._model_version = 0
        # Wait for the first weight sync before starting generation.
        self._generation_event = asyncio.Event()
        self._weights_refs = None
        self._timings_ms = {"ray_get": []}

    async def sync_weights(self, model_version, refs: List[ray.ObjectRef]):
        print("start syncing weights", model_version)

        # Pause generation.
        self._generation_event.clear()
        # Wait for in-flight generation requests to finish.
        if self._device.type == "cuda":
            torch.cuda.synchronize()

        # Sync weights.
        views = self._model.get_views()
        ref = refs[0]
        get_start = time.perf_counter()
        received_tensors = ray.get(ref)
        self._timings_ms["ray_get"].append((time.perf_counter() - get_start) * 1000.0)
        for view, received_tensor in zip(views, received_tensors):
            item = view[0].item()
            view.copy_(received_tensor)
            if not torch.all(item + 1 == view).item():
                print("weights not synced, got", view[0], "expected", item + 1)

        self._model_version = model_version
        print("synced weights", model_version)

        # Resume generation.
        self._generation_event.set()

    async def loop(self, num_iters: int):
        # Generation loop.
        inpt = torch.randn(
            self._model.layer.weight.shape[1], dtype=torch.float16, device=self._device
        )
        it = 0
        while True:
            # Check that generation is not paused.
            await self._generation_event.wait()

            time.sleep(0.1)
            output = self._model(inpt)
            print("generated output", output)

            # Yield.
            await asyncio.sleep(0)
            it += 1
            if it >= num_iters:
                break

    def get_timing_metrics(self):
        return _summarize_timings(self._timings_ms)


@ray.remote(enable_tensor_transport=True)
class Trainer:
    def __init__(self, use_nixl: bool, num_indices: int, device_str: str):
        self._device = torch.device(device_str)
        self._model = Model(num_indices).to(self._device)
        self._model_version = 0
        self._generators = []
        self._tensor_transport = "nixl" if use_nixl else None
        self._timings_ms = {"ray_put": [], "ray_get": []}

    async def reset_generators(self, generators: List[ActorHandle[Generator]]):
        self._generators = generators

    async def loop(self, generators: List[ActorHandle[Generator]], num_iters: int):
        """Training loop"""
        self._generators = generators

        it = 0
        while True:
            # Update weights.
            item = self._model.layer.weight[0][0].item()
            self._model.layer.weight += 1
            assert torch.all(
                item + 1 == self._model.layer.weight
            ).item(), "weights not updated"
            self._model_version += 1
            print("updated weights", self._model_version)

            # Put weights.
            views = self._model.get_views()
            put_start = time.perf_counter()
            weight_refs = [ray.put(views, _tensor_transport=self._tensor_transport)]
            self._timings_ms["ray_put"].append(
                (time.perf_counter() - put_start) * 1000.0
            )
            print("put weights", weight_refs)

            # Push weights to generators.
            sync_weights_tasks = []
            for generator in self._generators:
                print(
                    "syncing weights for version",
                    self._model_version,
                    "to generator",
                    generator,
                )
                sync_weights_tasks.append(
                    generator.sync_weights.remote(self._model_version, weight_refs)
                )
            del weight_refs
            if sync_weights_tasks:
                get_start = time.perf_counter()
                ray.get(sync_weights_tasks)
                self._timings_ms["ray_get"].append(
                    (time.perf_counter() - get_start) * 1000.0
                )

            # # Wait for RDT to finish the weight transfers.
            # # This waits until all generators that we pushed to in the previous round either finish the weight sync or the generator actor fails.
            # # After this returns, it's safe to update the local weights.
            # ray.experimental.wait_tensor_freed(views[0])

            it += 1
            if it >= num_iters:
                break

            # Yield to let the controller reset the list of generators.
            await asyncio.sleep(0)

    def get_timing_metrics(self):
        return _summarize_timings(self._timings_ms)


def _summarize_timings(timings_ms):
    summary = {}
    for name, values in timings_ms.items():
        if not values:
            summary[name] = {
                "mean_ms": None,
                "p90_ms": None,
                "p100_ms": None,
                "count": 0,
            }
            continue
        sorted_vals = sorted(values)
        count = len(sorted_vals)
        p90_idx = max(0, int(0.9 * count) - 1)
        summary[name] = {
            "mean_ms": mean(sorted_vals),
            "p90_ms": sorted_vals[p90_idx],
            "p100_ms": sorted_vals[-1],
            "count": count,
        }
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-indices", type=int, default=2)
    parser.add_argument("--use-nixl", action="store_true")
    parser.add_argument("--num-iters", type=int, default=10)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="cpu")
    parser.add_argument("--max-failures", type=int, default=1)
    args = parser.parse_args()

    num_indices = args.num_indices
    use_nixl = args.use_nixl
    num_iters = args.num_iters
    device_str = "cuda" if args.device == "gpu" else "cpu"
    max_failures = args.max_failures
    if num_indices > 5 and not use_nixl:
        raise SystemExit(
            "--num-indices > 5 requires --use-nixl. Using Ray object store will OOM."
        )

    ray.init()
    actor_opts = {"num_gpus": 1} if device_str == "cuda" else {}
    trainer = Trainer.options(**actor_opts).remote(use_nixl, num_indices, device_str)
    generator = Generator.options(**actor_opts).remote(num_indices, device_str)

    trainer_ref = trainer.loop.remote([generator], num_iters=num_iters)
    generator_ref = generator.loop.remote(num_iters=num_iters)

    failures = 0
    while failures < max_failures:
        # A ref will be ready if its actor process fails or if the loop exits (due to exception).
        failed, _ = ray.wait([trainer_ref, generator_ref], num_returns=1)
        if failed[0] is trainer_ref:
            try:
                ray.get(trainer_ref)
            except Exception as e:
                print("trainer failed", e)
                failures += 1
            # Start a new trainer.
            trainer = Trainer.options(**actor_opts).remote(
                use_nixl, num_indices, device_str
            )
            trainer_ref = trainer.loop.remote([generator], num_iters=num_iters)
        else:
            try:
                ray.get(generator_ref)
            except Exception as e:
                print("generator failed", e)
                failures += 1
            # Start a new generator.
            generator = Generator.options(**actor_opts).remote(num_indices, device_str)
            trainer.reset_generators.remote([generator])
            generator_ref = generator.loop.remote(num_iters=num_iters)

        trainer_metrics = ray.get(trainer.get_timing_metrics.remote())
        generator_metrics = ray.get(generator.get_timing_metrics.remote())
        print("trainer timing metrics", trainer_metrics)
        print("generator timing metrics", generator_metrics)
