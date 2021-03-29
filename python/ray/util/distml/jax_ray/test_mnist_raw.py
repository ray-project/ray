"""A basic MNIST example using Numpy and JAX.

Run ResNet18 in MNIST dataset on one GPU.
Test
"""
import time

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random, dlpack
from jax.tree_util import tree_flatten
from jax.experimental import optimizers
import jax.numpy as jnp
import datasets
from resnet import ResNet18, ResNet50, ResNet101
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
    def __init__(self, data, target, batch_size=128):
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

    def synth_batches(self):
        num_imgs = self.target.shape[0]
        rng = npr.RandomState(0)
        perm = rng.permutation(num_imgs)
        for i in range(self.num_batches):
            batch_idx = perm[i * self.batch_size:(i + 1) * self.batch_size]
            img_batch = self.data[:, :, :, batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()


class Worker:
    def __init__(self):
        rng_key = random.PRNGKey(0)

        self.batch_size = 128
        self.num_classes = 10
        self.input_shape = (28, 28, 1, self.batch_size)
        self.lr = 0.01
        self.num_epochs = 10

        init_fun, predict_fun = ResNet101(self.num_classes)
        _, init_params = init_fun(rng_key, self.input_shape)

        opt_init, opt_update, get_params = optimizers.adam(self.lr)

        opt_state = opt_init(init_params)

        self.opt_state = opt_state
        self.opt_update = opt_update
        self.get_params = get_params

        self.predict_fun = predict_fun

        self.steps = 0
        self.meter = TimeMeter()
        self.meter.set_world_size(1)

        def update(i, opt_state, batch):
            params = self.get_params(opt_state)
            gradient = grad(self.loss)(params, batch)

            return self.opt_update(i, gradient, opt_state)

        self.update = update # jax.jit(update)

    def set_dataloader(self, train_dataloader, test_dataloader):
        self.train_dataloader = train_dataloader
        self.test_dataloader = test_dataloader

    def set_jit(self, enable_jit):
        if enable_jit:
            self.update = jit(self.update)
            print("Worker runs in jit mode")

    def run(self):
        if not self.train_dataloader:
            raise RuntimeError("Train dataloader hasn't be set.")
        if not self.test_dataloader:
            raise RuntimeError("Test dataloader hasn't be set.")

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
            predicted_class = jnp.argmax(logits, axis=1)
            target_class = jnp.argmax(targets, axis=1)
            result.append(jnp.mean(predicted_class == target_class))
        return np.array(result).mean()


if __name__ == "__main__":
    os.environ["CUDA_VISIBLE_DEVICES"] = "7"
    enable_jit = False

    train_images, train_labels, test_images, test_labels = datasets.mnist()
    train_images = train_images.reshape(train_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)
    test_images = test_images.reshape(test_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)

    train_dataloader = Dataloader(train_images, train_labels, batch_size=128)
    test_dataloader = Dataloader(test_images, test_labels, batch_size=128)

    worker = Worker()
    print("worker init")
    worker.set_dataloader(train_dataloader, test_dataloader)
    worker.set_jit(enable_jit)

    worker.run()

