import os
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import torch
from ray.dag import InputNode

import ray
from ray.experimental.channel.torch_tensor_type import TorchTensorType

@ray.remote(num_cpus=0, num_gpus=1)
class BaseModelActor:
    def __init__(self):
        print("actor node id: ",ray.get_runtime_context().get_node_id())
        print("actor id: ",ray.get_runtime_context().get_actor_id())
        print(f"{os.environ.get('CUDA_VISIBLE_DEVICES')=}")

    def predict(self, prompt: str) -> torch.Tensor:
        print("BaseModelActor predict ")
        return torch.tensor([1], device="cpu")


@ray.remote(num_cpus=0, num_gpus=1)
class RefinerModelActor:
    def __init__(self):
        print("actor node id: ",ray.get_runtime_context().get_node_id())
        print("actor id: ",ray.get_runtime_context().get_actor_id())
        os.environ["CUDA_VISIBLE_DEVICES"] = "1"
        print(f"{os.environ.get('CUDA_VISIBLE_DEVICES')=}")

    def predict(self, prompt: str, image: torch.Tensor):
        print("RefinerModelActor predict ")
        image = image.to("cuda")
        return image

@ray.remote(num_cpus=1)
class SDXLModelParallel:
    def __init__(self):
        print("actor node id: ",ray.get_runtime_context().get_node_id())
        print("actor id: ",ray.get_runtime_context().get_actor_id())
        self._base_actor = BaseModelActor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False)).remote()
        self._refiner_actor = RefinerModelActor.remote()

        with InputNode() as prompt:
            image = self._base_actor.predict.bind(
                prompt)
            dag = self._refiner_actor.predict.bind(
                prompt,
                image.with_type_hint(TorchTensorType()),
            )

        print("compile starts")
        self._adag = dag.experimental_compile(
            _execution_timeout=120,
        )
        print("compile end")

    async def call(self, prompt: str) -> bytes:
        print("abc")
        return ray.get(self._adag.execute(prompt))
    

from ray.cluster_utils import Cluster

c = Cluster()
print("head node id: ", c.add_node(num_cpus=1, num_gpus=1).node_id)
ray.init()
print(c.add_node(num_cpus=0, num_gpus=1).node_id)
parallel = SDXLModelParallel.remote()
print(ray.get(parallel.call.remote("abc")))
breakpoint()
