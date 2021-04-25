"""A basic MNIST example using Numpy and JAX.

Run ResNet18 in MNIST dataset on one GPU.
Test
"""
import time

import numpy as np
import numpy.random as npr

import jax
from jax import jit, grad, random, dlpack
from jax.tree_util import tree_flatten, tree_unflatten
from jax.experimental import optimizers
import jax.numpy as jnp
from jax_util import datasets
from jax_util.resnet import ResNet18, ResNet50, ResNet101
import os

from ray.util.distml.util import ThroughoutCollection, func_timer

from tqdm import tqdm

import argparse

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

    def __len__(self):
        return self.num_batches


class Worker:
    def __init__(self, model_name):
        rng_key = random.PRNGKey(0)

        self.batch_size = 128
        self.num_classes = 10
        self.input_shape = (28, 28, 1, self.batch_size)
        self.lr = 0.01
        self.num_epochs = 10

        if model_name == "resnet18":
            init_fun, predict_fun = ResNet18(self.num_classes)
        elif model_name == "resnet50":
            init_fun, predict_fun = ResNet50(self.num_classes)
        elif model_name == "resnet101":
            init_fun, predict_fun = ResNet101(self.num_classes)
        else:
            raise RuntimeError("Unrecognized model name")
        
        _, init_params = init_fun(rng_key, self.input_shape)

        opt_init, opt_update, get_params = optimizers.adam(self.lr)

        opt_state = opt_init(init_params)

        self.opt_state = opt_state
        self.opt_update = opt_update
        self.get_params = get_params

        self.predict_fun = predict_fun

        self.steps = 0

        def update(i, opt_state, batch):
            params = self.get_params(opt_state)
            gradient = grad(self.loss)(params, batch)
            return self.opt_update(i, gradient, opt_state)

        self.update = update # jax.jit(update)

        self.collector = ThroughoutCollection(batch_size=self.batch_size, num_workers=1,
                                              job_name=f"mnist_benchmark_{model_name}")

    def derive_updates(self, batch):
        params = self.get_params(self.opt_state)
        gradient = grad(self.loss)(params, batch)
        return gradient

    def apply_updates(self, i, gradient):
         return self.opt_update(i, gradient, self.opt_state)

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
            for idx, batch in tqdm(enumerate(self.train_dataloader)):
                with self.collector.record("train_batch"):
                    self.opt_state = self.update(self.steps,
                                                 self.opt_state, 
                                                 batch)
                    # gradients = self.derive_updates(batch)
                    # self.opt_state = self.apply_updates(self.steps,
                    #                                     gradients)

                self.steps+=1

            epoch_time = time.time() - start_time
            test_start_time = time.time()
            params = self.get_params(self.opt_state)
            # train_acc = self.accuracy(params, self.train_dataloader)
            with self.collector.record("validate_epoch"):
                test_acc = self.accuracy(params, self.test_dataloader)
            self.collector.update("validate_epoch",val_acc=test_acc)
            self.collector.save("validate_epoch")
            
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for connecting to the Ray cluster")
    # parser.add_argument(
    #     "--num-workers",
    #     "-n",
    #     type=int,
    #     default=2,
    #     help="Sets number of workers for training.")
    parser.add_argument(
        "--num-epochs", type=int, default=20, help="Number of epochs to train.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")
    parser.add_argument(
        "--trainer", type=str, default="ar", help="Trainer type, Optional: ar, ps")
    parser.add_argument(
        "--model-name", type=str, default="resnet18", help="model, Optional: resnet18, resnet50, resnet101.")
    parser.add_argument(
        "--batch_size", type=int, default=128, help="batch size.")

    args = parser.parse_args()

    os.environ["CUDA_VISIBLE_DEVICES"] = "0"

    enable_jit = False

    train_images, train_labels, test_images, test_labels = datasets.mnist()
    train_images = train_images.reshape(train_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)
    test_images = test_images.reshape(test_images.shape[0], 1, 28, 28).transpose(2, 3, 1, 0)

    train_dataloader = Dataloader(train_images, train_labels, batch_size=args.batch_size)
    test_dataloader = Dataloader(test_images, test_labels, batch_size=args.batch_size)

    worker = Worker(args.model_name)
    print("worker init")
    worker.set_dataloader(train_dataloader, test_dataloader)
    worker.set_jit(enable_jit)

    worker.run()

