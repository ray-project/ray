import ray
import torch

ray.init()

assert torch.cuda.is_available()

