import argparse
import asyncio
import time
from typing import List

import torch

import ray
from ray.actor import ActorHandle

NUM_ITERS = 1000


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
        received_tensors = ray.get(ref)
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


@ray.remote(enable_tensor_transport=True)
class Trainer:
    def __init__(self, use_nixl: bool, num_indices: int, device_str: str):
        self._device = torch.device(device_str)
        self._model = Model(num_indices).to(self._device)
        self._model_version = 0
        self._generators = []
        self._tensor_transport = "nixl" if use_nixl else None

    async def set_generators(self, generators: List[ActorHandle[Generator]]):
        self._generators = generators

    async def loop(self, num_iters: int):
        """Training loop"""
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
            weight_refs = [ray.put(views, _tensor_transport=self._tensor_transport)]
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
                ray.get(sync_weights_tasks)

            # # Wait for RDT to finish the weight transfers.
            # # This waits until all generators that we pushed to in the previous round either finish the weight sync or the generator actor fails.
            # # After this returns, it's safe to update the local weights.
            # ray.experimental.wait_tensor_freed(views[0])

            it += 1
            if it >= num_iters:
                break

            # Yield to let the controller reset the list of generators.
            await asyncio.sleep(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-indices", type=int, default=2)
    parser.add_argument("--use-nixl", action="store_true")
    parser.add_argument("--num-iters", type=int, default=NUM_ITERS)
    parser.add_argument("--device", choices=["cpu", "gpu"], default="cpu")
    args = parser.parse_args()

    num_indices = args.num_indices
    use_nixl = args.use_nixl
    num_iters = args.num_iters
    device_str = "cuda" if args.device == "gpu" else "cpu"
    if num_indices > 5 and not use_nixl:
        raise SystemExit(
            "--num-indices > 5 requires --use-nixl. Using Ray object store will OOM."
        )

    ray.init()
    trainer = Trainer.remote(use_nixl, num_indices, device_str)
    generator = Generator.remote(num_indices, device_str)

    trainer.set_generators.remote([generator])
    trainer_ref = trainer.loop.remote(num_iters=num_iters)
    generator_ref = generator.loop.remote(num_iters=num_iters)

    while True:
        # A ref will be ready if its actor process fails or if the loop exits (due to exception).
        failed, _ = ray.wait([trainer_ref, generator_ref], num_returns=1)
        if failed[0] is trainer_ref:
            try:
                ray.get(trainer_ref)
            except Exception as e:
                print("trainer failed", e)
            # Start a new trainer.
            trainer = Trainer.remote(use_nixl, num_indices, device_str)
            trainer.set_generators.remote([generator])
            trainer_ref = trainer.loop.remote(num_iters=num_iters)
        else:
            try:
                ray.get(generator_ref)
            except Exception as e:
                print("generator failed", e)
            # Start a new generator.
            generator = Generator.remote(num_indices, device_str)
            trainer.set_generators.remote([generator])
            generator_ref = generator.loop.remote(num_iters=num_iters)
