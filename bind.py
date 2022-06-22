import ray
from ray import serve

import torch


@ray.remote
def torch_tensor(size: int):
    return torch.rand(size)


@serve.deployment(num_replicas=2)
class MyModel:
    def __init__(self, model):
        self._model = model

    def __call__(self, *args):
        encoded_input = self._tokenizer("hello friend", return_tensors="pt")
        return self._model(**encoded_input)


m = MyModel.bind(torch_tensor.bind(1024 ** 3))
