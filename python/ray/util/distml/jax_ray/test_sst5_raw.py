"""A basic MNIST example using Numpy and JAX.

The primary aim here is simplicity and minimal dependencies.
"""
import time

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten
from jax.lib import xla_client
from jax.experimental import optimizers
import jax.numpy as jnp
import jax.experimental.stax as stax

import ray
import ray.util.collective as col
import cupy as cp
import os

import functools

from sst import make_sst5_dataloader

from model_transformer import transformer, create_root_context, print_variables


class TimeMeter:
    def __init__(self):
        self.world_size = 1
        self.batch_size = None
        self.time = 0
        self.steps = 0
        self.data_num = 0

    def set_world_size(self, world_size):
        self.world_size = world_size

    def start_timer(self):
        self.start_time = time.time()

    def stop_timer(self):
        self.time += time.time() - self.start_time
        self.start_time = None

    def step(self, batch_size):
        self.data_num += batch_size * self.world_size
        self.steps += 1
        self.time += time.time() - self.start_time
        self.start_time = time.time()
        if not self.steps % 200:
            print(f"Step {self.steps} passing {self.data_num} data after {self.time} sec",
                    "Throughout: {:.2f}".format(self.data_num/self.time))


class Worker:
    def __init__(self, train_dataloader, test_dataloader, world_size=1):
        self.train_dataloader = train_dataloader
        self.test_dataloader = test_dataloader

        rng_key = random.PRNGKey(0)

        self.batch_size = 128
        self.lr = 0.01
        self.num_epochs = 100
        self.world_size = world_size

        n_ctx = 256  # length
        n_head = 4
        n_layer = 8
        n_embd = 256
        self.model = functools.partial(transformer, n_vocab=30522,
            n_head=n_head, n_layer=n_layer, n_ctx=n_ctx, n_embd=n_embd)

        root_cx = create_root_context()

        batch = next(iter(train_dataloader))
        self.loss(root_cx, batch) # Just create variables
        root_cx.allow_new = False
        # print_variables(root_cx)
        init_params = root_cx.variables_list()
        opt_init, opt_update, get_params = optimizers.adam(self.lr*world_size)

        opt_state = opt_init(init_params)

        self.root_cx = root_cx
        self.opt_state = opt_state
        self.opt_update = opt_update
        self.get_params = get_params

        self.steps = 0
        self.meter = TimeMeter()

        def update(i, opt_state, batch):
            params = get_params(opt_state)
            v, gradient = jax.value_and_grad(self.loss2)(params, batch)
            return v, self.opt_update(i, gradient, opt_state)

        self.update = update

    def set_jit(self, enable_jit=False):
        if enable_jit:
            print("Enbale jit.")
            self.update = jit(self.update)

    def get_jax_dlpack(self, tensor):
        return xla_client._xla.buffer_to_dlpack_managed_tensor(
            tensor.device_buffer, take_ownership=False
        )

    def run(self):
        if not self.train_dataloader:
            raise RuntimeError("Train dataloader hasn't been set.")
        if not self.test_dataloader:
            raise RuntimeError("Test dataloader hasn't been set.")

        for epoch in range(self.num_epochs):
            start_time = time.time()
            self.meter.start_timer()

            for idx, batch in enumerate(self.train_dataloader):
                lossval, self.opt_state = self.update(self.steps,
                                             self.opt_state,
                                             batch)
                self.steps+=1
                self.meter.step(batch[1].shape[0])
            self.meter.stop_timer()
            epoch_time = time.time() - start_time
            test_start_time = time.time()
            params = self.get_params(self.opt_state)
            # train_acc = self.accuracy(params, self.train_dataloader)
            test_acc = self.accuracy(params, self.test_dataloader)
            test_time = time.time() - test_start_time
            print("Epoch {} in {:0.2f} sec, test time {:0.2f} sec."
                .format(epoch, epoch_time, test_time))
            # print("Training set accuracy {}".format(train_acc))
            print("Test set accuracy {}".format(test_acc))

    def loss(self, cx, batch):
        input, target = batch
        logprobs_btq = self.model(cx, input[:, :-1])
        return -jnp.sum(logprobs_btq * target)

    def loss2(self, params, batch):
        cx = self.root_cx.replace_with_list(params)
        return self.loss(cx, batch)

    def accuracy(self, params, dataloader):
        result = []
        cx = self.root_cx.replace_with_list(params)
        for _, (inputs, targets) in enumerate(dataloader):
            logits = self.model(cx, inputs[:, :-1])
            predicted_class = jnp.argmax(logits, axis=-1)
            target_class = jnp.argmax(targets, axis=-1)
            result += list(predicted_class == target_class)
        return np.mean(result)


if __name__ == '__main__':
    gpu_ids = [3]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
    num_gpus = len(gpu_ids)
    enable_jit = False

    train_dataloader, val_dataloader, test_dataloader = make_sst5_dataloader()

    actor = Worker(train_dataloader, val_dataloader)
    print("worker init")
    actor.set_jit(enable_jit)

    actor.run()
