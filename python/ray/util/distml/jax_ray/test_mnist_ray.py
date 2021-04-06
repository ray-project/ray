"""A basic MNIST example using Numpy and JAX.

The primary aim here is simplicity and minimal dependencies.
"""
import time

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
from jax.lib import xla_client
import jax.numpy as jnp
import datasets
from resnet import ResNet18, ResNet50, ResNet101

import ray
import ray.util.collective as col
import cupy as cp
import os


class TimeMeter:
    def __init__(self):
        self.world_size = None
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


class Dataloader:
    def __init__(self, data, target, batch_size=128, shuffle=False):
        '''
        data: shape(width, height, channel, num)
        target: shape(num, num_classes)
        '''
        self.data = data
        self.target = target
        self.batch_size = batch_size
        num_data = self.target.shape[0]
        num_complete_batches, leftover = divmod(num_data, batch_size)
        self.num_batches = num_complete_batches + bool(leftover)
        self.shuffle = shuffle

    def synth_batches(self):
        num_imgs = self.target.shape[0]
        rng = npr.RandomState(np.random.randint(10))
        perm = rng.permutation(num_imgs) if self.shuffle else np.arange(num_imgs)
        for i in range(self.num_batches):
            batch_idx = perm[i * self.batch_size:(i + 1) * self.batch_size]
            img_batch = self.data[:, :, :, batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()


@ray.remote(num_gpus=1, num_cpus=1)
class Worker:
    def __init__(self, world_size=1):
        rng_key = random.PRNGKey(0)

        self.batch_size = 128
        self.num_classes = 10
        self.input_shape = (28, 28, 1, self.batch_size)
        self.lr = 0.01
        self.num_epochs = 10
        self.world_size = world_size

        init_fun, predict_fun = ResNet18(self.num_classes)
        _, init_params = init_fun(rng_key, self.input_shape)

        opt_init, opt_update, get_params = optimizers.adam(self.lr*world_size)

        opt_state = opt_init(init_params)

        self.opt_state = opt_state
        self.opt_update = opt_update
        self.get_params = get_params

        self.predict_fun = predict_fun

        self.steps = 0
        self.meter = TimeMeter()

        def update_without_jit(i, opt_state, batch):
            params = self.get_params(opt_state)
            gradient = grad(self.loss)(params, batch)

            ftree, _ = tree_flatten(gradient)
            for g in ftree:
                if not len(g):
                    continue
                cp_g = cp.fromDlpack(self.get_jax_dlpack(g))
                col.allreduce(cp_g, group_name="default")
                cp_g/=self.world_size
            return self.opt_update(i, gradient, opt_state)

        def update_with_jit(i, opt_state, batch):
            @jit
            def part1(opt_state, batch):
                params = self.get_params(opt_state)
                return grad(self.loss)(params, batch)
            @jit
            def part2(i, gradient, opt_state):
                return self.opt_update(i, gradient, opt_state)

            gradient = part1(opt_state, batch)
            ftree, tree = tree_flatten(gradient)
            for g in ftree:
                if not len(g):
                    continue
                cp_g = cp.fromDlpack(self.get_jax_dlpack(g))
                col.allreduce(cp_g, group_name="default")
                cp_g/=self.world_size
            return part2(i, gradient, opt_state)

        self.update_without_jit = update_without_jit
        self.update_with_jit = update_with_jit

    def init_group(self,
                   world_size,
                   rank,
                   backend="nccl",
                   group_name="default"):
        col.init_collective_group(world_size, rank, backend, group_name)
        ftree, _ = tree_flatten(self.get_params(self.opt_state))

        for p in ftree:
            cp_p = cp.fromDlpack(self.get_jax_dlpack(p))
            col.allreduce(cp_p, group_name="default")
            cp_p/=world_size

        self.world_size = world_size
        self.meter.set_world_size(world_size)

    def set_dataloader(self, train_dataloader, test_dataloader):
        self.train_dataloader = train_dataloader
        self.test_dataloader = test_dataloader

    def set_jit(self, enable_jit=False):
        if enable_jit:
            print("Enbale jit.")
            self.update = self.update_with_jit
        else:
            self.update = self.update_without_jit

    def get_jax_dlpack(self, tensor):
        return xla_client._xla.buffer_to_dlpack_managed_tensor(tensor.device_buffer,
                                                               take_ownership=False)

    def run(self):
        if not self.train_dataloader:
            raise RuntimeError("Train dataloader hasn't been set.")
        if not self.test_dataloader:
            raise RuntimeError("Test dataloader hasn't been set.")

        for epoch in range(self.num_epochs):
            start_time = time.time()
            self.meter.start_timer()
            for idx, batch in enumerate(self.train_dataloader):
                self.opt_state = self.update(self.steps,
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

    def loss(self, params, batch):
        inputs, targets = batch
        logits = self.predict_fun(params, inputs)
        return -jnp.sum(logits * targets)

    def accuracy(self, params, dataloader):
        result = []
        for _, (inputs, targets) in enumerate(dataloader):
            logits = self.predict_fun(params, inputs)
            predicted_class = jnp.argmax(logits, axis=-1)
            target_class = jnp.argmax(targets, axis=-1)
            result += list(predicted_class == target_class)
        return np.mean(result)


if __name__ == "__main__":
    gpu_ids = [0,4]
    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(map(str, gpu_ids))
    num_gpus = len(gpu_ids)
    enable_jit = False

    ray.init(num_gpus=num_gpus, num_cpus=6)

    train_images, train_labels, test_images, test_labels = datasets.mnist()
    train_images = train_images.reshape(train_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)
    test_images = test_images.reshape(test_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)

    train_dataloader = Dataloader(train_images, train_labels, batch_size=128, shuffle=True)
    test_dataloader = Dataloader(test_images, test_labels, batch_size=128)

    actors = [Worker.remote(num_gpus) for _ in range(num_gpus)]
    print("worker init")
    ray.get([actor.init_group.remote(num_gpus, rank, group_name="default")
             for rank, actor in enumerate(actors)])
    print("worker init_group")
    ray.get([actor.set_dataloader.remote(train_dataloader, test_dataloader)
            for actor in actors])
    ray.get([actor.set_jit.remote(enable_jit)
            for actor in actors])

    ray.get([actor.run.remote() for actor in actors])

