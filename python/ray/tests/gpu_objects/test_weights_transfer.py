import argparse
import asyncio
import json
import time
from statistics import mean
from typing import List

import torch

import ray
from ray.actor import ActorHandle
from ray.experimental import register_nixl_memory, set_target_for_ref


class Model(torch.nn.Module):
    def __init__(self, num_indices: int):
        super().__init__()
        # 2GiB matrix.
        TOTAL_SIZE_BYTES = 2 * 100 * 1024 * 1024
        NUM_ROWS = 1_000
        self.layer = torch.nn.Linear(
            TOTAL_SIZE_BYTES // NUM_ROWS // 2, NUM_ROWS, dtype=torch.float16
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
    def __init__(self, use_nixl: bool, num_indices: int, device_str: str):
        init_start = time.perf_counter()
        self._device = torch.device(device_str)
        self._model = Model(num_indices).to(self._device)
        self._model_version = 0
        if use_nixl:
            register_nixl_memory(self._model.layer.weight)
        # Wait for the first weight sync before starting generation.
        self._generation_event = asyncio.Event()
        self._timings_ms = {"ray_get": []}
        self._timings_ms["Generator.__init__"] = [
            (time.perf_counter() - init_start) * 1000.0
        ]

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
        old_items = [view[0].item() for view in views]
        get_start = time.perf_counter()
        set_target_for_ref(ref, views)
        ray.get(ref)
        self._timings_ms["ray_get"].append((time.perf_counter() - get_start) * 1000.0)
        for view, old_item in zip(views, old_items):
            if not torch.all(old_item + 1 == view).item():
                print("weights not synced, got", view[0], "expected", old_item + 1)

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

    def get_gpu_memory_metrics(self):
        if self._device.type == "cuda":
            return {
                "peak_allocated_bytes": torch.cuda.max_memory_allocated(self._device),
                "peak_reserved_bytes": torch.cuda.max_memory_reserved(self._device),
            }
        return {}

    def get_nixl_transport_metrics(self):
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
        )

        transport = get_tensor_transport_manager("NIXL")
        return {
            "get_xfer_descs_times_s": transport.get_xfer_descs_cost,
            "serialize_times_s": transport.ser_cost_map,
            "deserialize_times_s": transport.deser_cost_map,
        }


@ray.remote(enable_tensor_transport=True)
class Trainer:
    def __init__(self, use_nixl: bool, num_indices: int, device_str: str):
        init_start = time.perf_counter()
        self._device = torch.device(device_str)
        self._model = Model(num_indices).to(self._device)
        self._model_version = 0
        self._generators = []
        self._tensor_transport = "nixl" if use_nixl else None
        if use_nixl:
            register_nixl_memory(self._model.layer.weight)
        self._timings_ms = {"ray_put": [], "ray_get": []}
        self._timings_ms["Trainer.__init__"] = [
            (time.perf_counter() - init_start) * 1000.0
        ]

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

    def get_bytes_per_iteration(self) -> int:
        views = self._model.get_views()
        return sum(view.numel() * view.element_size() for view in views)

    def get_gpu_memory_metrics(self):
        if self._device.type == "cuda":
            return {
                "peak_allocated_bytes": torch.cuda.max_memory_allocated(self._device),
                "peak_reserved_bytes": torch.cuda.max_memory_reserved(self._device),
            }
        return {}

    def get_nixl_transport_metrics(self):
        from ray.experimental.gpu_object_manager.util import (
            get_tensor_transport_manager,
        )

        transport = get_tensor_transport_manager("NIXL")
        return {
            "get_xfer_descs_times_s": transport.get_xfer_descs_cost,
            "serialize_times_s": transport.ser_cost_map,
            "deserialize_times_s": transport.deser_cost_map,
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-indices", type=int, default=2)
    parser.add_argument("--use-nixl", action="store_true")
    parser.add_argument("--num-iters", type=int, default=10)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="cpu")
    parser.add_argument("--max-trainer-failures", type=int, default=0)
    parser.add_argument("--max-generator-failures", type=int, default=0)
    parser.add_argument("--kill-interval-s", type=int, default=30)
    parser.add_argument("--output-file", required=True)
    args = parser.parse_args()

    num_indices = args.num_indices
    use_nixl = args.use_nixl
    num_iters = args.num_iters
    device_str = "cuda" if args.device == "gpu" else "cpu"
    max_trainer_failures = args.max_trainer_failures
    max_generator_failures = args.max_generator_failures
    if num_indices > 5 and not use_nixl:
        raise SystemExit(
            "--num-indices > 5 requires --use-nixl. Using Ray object store will OOM."
        )

    ray.init()
    run_start = time.perf_counter()
    actor_opts = {"num_gpus": 1} if device_str == "cuda" else {}
    trainer = Trainer.options(**actor_opts).remote(use_nixl, num_indices, device_str)
    generator = Generator.options(**actor_opts).remote(
        use_nixl, num_indices, device_str
    )

    bytes_per_iteration = ray.get(trainer.get_bytes_per_iteration.remote())
    trainer_ref = trainer.loop.remote([generator], num_iters=num_iters)
    generator_ref = generator.loop.remote(num_iters=num_iters)

    trainer_failures = 0
    generator_failures = 0
    time_since_last_kill = time.time()
    num_trainer_kills = 0
    num_generator_kills = 0

    while (
        trainer_failures < max_trainer_failures
        and generator_failures < max_generator_failures
    ):
        # A ref will be ready if its actor process fails or if the loop exits (due to exception).
        # Kill every 30s.
        failed, _ = ray.wait(
            [trainer_ref, generator_ref], num_returns=1, timeout=args.kill_interval_s
        )

        if time.time() - time_since_last_kill > 30:
            if num_trainer_kills < max_trainer_failures:
                print("killing trainer")
                ray.kill(trainer, force=True)
                num_trainer_kills += 1
            if num_generator_kills < max_generator_failures:
                print("killing generator")
                ray.kill(generator, force=True)
                num_generator_kills += 1
            time_since_last_kill = time.time()

        if failed[0] is trainer_ref:
            try:
                ray.get(trainer_ref)
                break
            except Exception as e:
                print("trainer failed", e)
                trainer_failures += 1
            # Start a new trainer.
            print("starting new trainer")
            trainer = Trainer.options(**actor_opts).remote(
                use_nixl, num_indices, device_str
            )
            trainer_ref = trainer.loop.remote([generator], num_iters=num_iters)
        else:
            try:
                ray.get(generator_ref)
                break
            except Exception as e:
                print("generator failed", e)
                generator_failures += 1
            # Start a new generator.
            print("starting new generator")
            generator = Generator.options(**actor_opts).remote(
                use_nixl, num_indices, device_str
            )
            trainer.reset_generators.remote([generator])
            generator_ref = generator.loop.remote(num_iters=num_iters)

    ray.get([trainer_ref, generator_ref])
    run_time_ms = (time.perf_counter() - run_start) * 1000.0
    print("job complete")
    trainer_metrics = ray.get(trainer.get_timing_metrics.remote())
    generator_metrics = ray.get(generator.get_timing_metrics.remote())
    trainer_gpu_memory = ray.get(trainer.get_gpu_memory_metrics.remote())
    generator_gpu_memory = ray.get(generator.get_gpu_memory_metrics.remote())
    trainer_nixl_metrics = ray.get(trainer.get_nixl_transport_metrics.remote())
    generator_nixl_metrics = ray.get(generator.get_nixl_transport_metrics.remote())

    output = {
        "num_indices": num_indices,
        "num_iters": num_iters,
        "num_bytes_per_iter": bytes_per_iteration,
        "device": args.device,
        "use_nixl": use_nixl,
        "max_trainer_failures": max_trainer_failures,
        "max_generator_failures": max_generator_failures,
        "num_trainer_kills": num_trainer_kills,
        "num_generator_kills": num_generator_kills,
        "total_run_time_ms": run_time_ms,
        "trainer_metrics": trainer_metrics,
        "generator_metrics": generator_metrics,
        "trainer_nixl_metrics": {
            **_summarize_timings(
                {
                    "get_xfer_descs": [
                        t * 1000.0
                        for t in trainer_nixl_metrics["get_xfer_descs_times_s"]
                    ],
                    "serialize": [
                        t * 1000.0 for t in trainer_nixl_metrics["serialize_times_s"]
                    ],
                }
            ),
            "get_xfer_descs_sum_s": sum(trainer_nixl_metrics["get_xfer_descs_times_s"]),
            "serialize_sum_s": sum(trainer_nixl_metrics["serialize_times_s"]),
        },
        "generator_nixl_metrics": {
            **_summarize_timings(
                {
                    "deserialize": [
                        t * 1000.0
                        for t in generator_nixl_metrics["deserialize_times_s"]
                    ],
                }
            ),
            "deserialize_sum_s": sum(generator_nixl_metrics["deserialize_times_s"]),
        },
        "trainer_gpu_memory": trainer_gpu_memory,
        "generator_gpu_memory": generator_gpu_memory,
    }
    print(output)
    with open(args.output_file, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(output) + "\n")
