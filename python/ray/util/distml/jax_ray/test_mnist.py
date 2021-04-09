"""A basic MNIST example using Numpy and JAX.

The primary aim here is simplicity and minimal dependencies.
"""


import time
import os
import numpy as np
import numpy.random as npr

from jax import jit, grad, random
from jax.experimental import optimizers
from jax.scipy.special import logsumexp
import jax.numpy as jnp
import datasets
from resnet import ResNet18


def loss(params, batch):
    inputs, targets = batch
    logits = predict_fun(params, inputs)
    return -jnp.sum(logits * targets)


def synth_batches(images, labels, num_b):
    num_imgs = labels.shape[0]
    rng = npr.RandomState(0)
    while True:
        perm = rng.permutation(num_imgs)
        for i in range(num_b):
            batch_idx = perm[i * batch_size:(i + 1) * batch_size]
            img_batch = images[:,:,:, batch_idx]
            label_batch = labels[batch_idx]
            yield img_batch, label_batch


def accuracy(params, batch):
    num_imgs = batch[1].shape[0]
    num_complete_batches, leftover = divmod(num_imgs, batch_size)
    num_batches = num_complete_batches + bool(leftover)
    batches = synth_batches(batch[0], batch[1], batch_size)

    result = []
    for i in range(num_batches):
        inputs, targets = next(batches)
        target_class = jnp.argmax(targets, axis=1)
        logits = predict_fun(params, inputs)
        predicted_class = jnp.argmax(logits, axis=1)
        result.append(jnp.mean(predicted_class == target_class))
    return np.array(result).mean()


if __name__ == "__main__":
    os.environ["CUDA_VISIBLE_DEVICES"] = "7"

    rng_key = random.PRNGKey(0)

    batch_size = 128
    num_classes = 10
    input_shape = (28, 28, 1, batch_size)
    lr = 0.01
    num_epochs = 10

    init_fun, predict_fun = ResNet18(num_classes)
    _, init_params = init_fun(rng_key, input_shape)

    opt_init, opt_update, get_params = optimizers.adam(lr)

    opt_state = opt_init(init_params)

    train_images, train_labels, test_images, test_labels = datasets.mnist()
    num_train = train_images.shape[0]
    num_test = test_images.shape[0]
    train_images = train_images.reshape(num_train, 1, 28, 28).transpose(2,3,1,0)
    test_images = test_images.reshape(num_test, 1, 28, 28).transpose(2,3,1,0)
    num_complete_batches, leftover = divmod(num_train, batch_size)
    num_batches = num_complete_batches + bool(leftover)

    # @jit
    def update(i, opt_state, batch):
        params = get_params(opt_state)
        return opt_update(i, grad(loss)(params, batch), opt_state)

    batches = synth_batches(train_images, train_labels, num_batches)
    i = 0
    for epoch in range(num_epochs):
        start_time = time.time()
        for _ in range(num_batches):
            opt_state = update(i, opt_state, next(batches))
            i+=1
        epoch_time = time.time() - start_time
        test_start_time = time.time()
        params = get_params(opt_state)
        train_acc = accuracy(params, (train_images, train_labels))
        test_acc = accuracy(params, (test_images, test_labels))
        test_time = time.time() - test_start_time
        print("Epoch {} in {:0.2f} sec, test time {:0.2f} sec."
               .format(epoch, epoch_time, test_time))
        print("Training set accuracy {}".format(train_acc))
        print("Test set accuracy {}".format(test_acc))
    # get_params(opt_state)