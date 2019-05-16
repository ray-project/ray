from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch

import ray

from ray.experimental.sgd.pytorch.utils import Resources
from ray.experimental.sgd.pytorch.pytorch_distributed_sgd import PyTorchDistributedSGD
from ray.experimental.sgd.pytorch.test.test_utils import model_creator, data_creator, optimizer_creator

ray.init()

sgd1 = PyTorchDistributedSGD(model_creator,
                             data_creator,
                             optimizer_creator,
                             num_replicas=3)
print(sgd1.train())
sgd1.save("/tmp/distributed_pytorch_checkpoint")
m1 = sgd1.get_model()

sgd2 = PyTorchDistributedSGD(model_creator,
                             data_creator,
                             optimizer_creator,
                             num_replicas=3)
sgd2.restore("/tmp/distributed_pytorch_checkpoint")
m2 = sgd2.get_model()

x = torch.from_numpy(np.ones((1, 1), dtype=np.float32))
assert m1(x) == m2(x)
print("Passed test")