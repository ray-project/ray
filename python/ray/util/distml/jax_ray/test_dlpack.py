# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A mock-up showing a ResNet50 network with training on synthetic data.

This file uses the stax neural network definition library and the optimizers
optimization library.
"""

from jax._src.dlpack import to_dlpack, from_dlpack
import numpy.random as npr
from jax.lib import xla_bridge, xla_client

import jax.numpy as jnp
from jax import jit, grad, random
from jax.experimental import optimizers
from jax.experimental import stax
from jax.experimental.stax import (AvgPool, BatchNorm, Conv, Dense, FanInSum,
                                   FanOut, Flatten, GeneralConv, Identity,
                                   MaxPool, Relu, LogSoftmax)
from jax.tree_util import tree_flatten
from torch.utils import dlpack
# ResNet blocks compose other layers

def ConvBlock(kernel_size, filters, strides=(2, 2)):
  ks = kernel_size
  filters1, filters2, filters3 = filters
  Main = stax.serial(
      Conv(filters1, (1, 1), strides), BatchNorm(), Relu,
      Conv(filters2, (ks, ks), padding='SAME'), BatchNorm(), Relu,
      Conv(filters3, (1, 1)), BatchNorm())
  Shortcut = stax.serial(Conv(filters3, (1, 1), strides), BatchNorm())
  return stax.serial(FanOut(2), stax.parallel(Main, Shortcut), FanInSum, Relu)


def IdentityBlock(kernel_size, filters):
  ks = kernel_size
  filters1, filters2 = filters
  def make_main(input_shape):
    # the number of output channels depends on the number of input channels
    return stax.serial(
        Conv(filters1, (1, 1)), BatchNorm(), Relu,
        Conv(filters2, (ks, ks), padding='SAME'), BatchNorm(), Relu,
        Conv(input_shape[3], (1, 1)), BatchNorm())
  Main = stax.shape_dependent(make_main)
  return stax.serial(FanOut(2), stax.parallel(Main, Identity), FanInSum, Relu)


# ResNet architectures compose layers and ResNet blocks

def ResNet50(num_classes):
  return stax.serial(
      GeneralConv(('HWCN', 'OIHW', 'NHWC'), 64, (7, 7), (2, 2), 'SAME'),
      BatchNorm(), Relu, MaxPool((3, 3), strides=(2, 2)),
      ConvBlock(3, [64, 64, 256], strides=(1, 1)),
      IdentityBlock(3, [64, 64]),
      IdentityBlock(3, [64, 64]),
      ConvBlock(3, [128, 128, 512]),
      IdentityBlock(3, [128, 128]),
      IdentityBlock(3, [128, 128]),
      IdentityBlock(3, [128, 128]),
      ConvBlock(3, [256, 256, 1024]),
      IdentityBlock(3, [256, 256]),
      IdentityBlock(3, [256, 256]),
      IdentityBlock(3, [256, 256]),
      IdentityBlock(3, [256, 256]),
      IdentityBlock(3, [256, 256]),
      ConvBlock(3, [512, 512, 2048]),
      IdentityBlock(3, [512, 512]),
      IdentityBlock(3, [512, 512]),
      AvgPool((7, 7)), Flatten, Dense(num_classes), LogSoftmax)


if __name__ == "__main__":
  import cupy as cp
  rng_key = random.PRNGKey(0)

  batch_size = 8
  num_classes = 1001
  input_shape = (224, 224, 3, batch_size)
  step_size = 0.1
  num_steps = 10

  init_fun, predict_fun = ResNet50(num_classes)
  _, init_params = init_fun(rng_key, input_shape)

  opt_init, opt_update, get_params = optimizers.momentum(step_size, mass=0.9)

  opt_state = opt_init(init_params)

  params = get_params(opt_state)

  flatten_params, tree = tree_flatten(params)

  count_cp = 0
  count_torch = 0
  for idx, p in enumerate(flatten_params):
    p_ptr = p.device_buffer.unsafe_buffer_pointer()

    backend = xla_bridge.get_backend()
    client = getattr(backend, "client", backend)

    # buf = xla_client._xla.dlpack_managed_tensor_to_buffer(p, client)
    buf = xla_client._xla.buffer_to_dlpack_managed_tensor(p.device_buffer, take_ownership=False)

    cp_p = cp.fromDlpack(buf)
    cp_ptr = cp_p.data.ptr

    # torch_p = dlpack.from_dlpack(buf)
    # torch_ptr = torch_p.data_ptr()

    p_new = from_dlpack(cp_p.toDlpack())
    p_new_ptr = p.device_buffer.unsafe_buffer_pointer()

    # cp_p = cp.fromDlpack(to_dlpack(p))
    # cp_p += 1
    # cp_ptr = cp_p.data.ptr
    # torch_p = dlpack.from_dlpack(to_dlpack(p))
    # torch_ptr = torch_p.data_ptr()

    # p_new = from_dlpack(cp_p.toDlpack())
    # p_new_ptr = p.device_buffer.unsafe_buffer_pointer()

    # p

    count_cp += 1 if cp_p.data.ptr!=p.device_buffer.unsafe_buffer_pointer() else 0
    # count_torch += 1 if torch_p.data_ptr()!=p.device_buffer.unsafe_buffer_pointer() else 0

  print(count_cp, len(flatten_params))
  print(count_torch, len(flatten_params))