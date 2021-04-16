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

"""Datasets used in examples."""


import array
import gzip
import os
from os import path
import struct
import urllib.request
from jax.api import F
import jax.numpy as jnp
from jax import jit

import numpy as np
import numpy.random as npr
import pickle
from functools import partial

_DATA = "/tmp/jax_example_data/"


def _download(url, filename, dataset_name="mnist"):
    """Download a url to a file in the JAX data temp directory."""
    root = os.path.join(_DATA,dataset_name)
    if not path.exists(root):
        os.makedirs(root)
    out_file = path.join(root, filename)
    if not path.isfile(out_file):
        urllib.request.urlretrieve(url, out_file)
        print("downloaded {} to {}".format(url, root))


def _partial_flatten(x):
    """Flatten all but the first dimension of an ndarray."""
    return np.reshape(x, (x.shape[0], -1))


def _one_hot(x, k, dtype=np.float32):
    """Create a one-hot encoding of x of size k."""
    return np.asarray(x[:, None] == np.arange(k), dtype)

# @partial(jit, static_argnums=1)
def _one_hot_jit(x, k, dtype=np.float32):
    """Create a one-hot encoding of x of size k."""
    return jnp.asarray(x[:, None] == jnp.arange(0, k), dtype)

def mnist_raw():
    """Download and parse the raw MNIST dataset."""
    # CVDF mirror of http://yann.lecun.com/exdb/mnist/
    base_url = "https://storage.googleapis.com/cvdf-datasets/mnist/"

    def parse_labels(filename):
        with gzip.open(filename, "rb") as fh:
            _ = struct.unpack(">II", fh.read(8))
            return np.array(array.array("B", fh.read()), dtype=np.uint8)

    def parse_images(filename):
        with gzip.open(filename, "rb") as fh:
            _, num_data, rows, cols = struct.unpack(">IIII", fh.read(16))
            return np.array(array.array("B", fh.read()),
                            dtype=np.uint8).reshape(num_data, rows, cols)

    for filename in ["train-images-idx3-ubyte.gz", "train-labels-idx1-ubyte.gz",
                    "t10k-images-idx3-ubyte.gz", "t10k-labels-idx1-ubyte.gz"]:
        _download(base_url + filename, filename)

    train_images = parse_images(path.join(_DATA, "mnist", "train-images-idx3-ubyte.gz"))
    train_labels = parse_labels(path.join(_DATA, "mnist", "train-labels-idx1-ubyte.gz"))
    test_images = parse_images(path.join(_DATA, "mnist", "t10k-images-idx3-ubyte.gz"))
    test_labels = parse_labels(path.join(_DATA, "mnist", "t10k-labels-idx1-ubyte.gz"))

    return train_images, train_labels, test_images, test_labels


def mnist(permute_train=False):
    """Download, parse and process MNIST data to unit scale and one-hot labels."""
    train_images, train_labels, test_images, test_labels = mnist_raw()

    train_images = _partial_flatten(train_images) / np.float32(255.)
    test_images = _partial_flatten(test_images) / np.float32(255.)
    train_labels = _one_hot(train_labels, 10)
    test_labels = _one_hot(test_labels, 10)

    if permute_train:
        perm = np.random.RandomState(0).permutation(train_images.shape[0])
        train_images = train_images[perm]
        train_labels = train_labels[perm]

    return train_images, train_labels, test_images, test_labels

def cifa100_raw():
    """Download and parse the raw MNIST dataset."""
    base_url = "http://www.cs.toronto.edu/~kriz/"

    def load_CIFAR_batch(root, mode="train"):
        """ load single batch of cifar """
        if mode == "train":
            filename = path.join(root, "train")
        elif mode == "test":
            filename = path.join(root, "test")
        else:
            raise RuntimeError("Error: unrecognized mode",
                               " Got {}".format(mode))

        with open(filename, 'rb')as f:
            datadict = pickle.load(f,encoding='bytes')
            X = datadict[b'data']
            Y = datadict[b'fine_labels']
            if mode == "train":
                X = X.reshape(50000, 3, 32, 32)
            else:
                X = X.reshape(10000, 3, 32, 32)
        return np.array(X), np.array(Y)

    for filename in ["cifar-100-python.tar.gz"]:
        _download(base_url + filename, filename, dataset_name="cifa100")

    root = path.join(_DATA, "cifa100")

    if not os.path.exists(path.join(root, "cifar-100-python.tar.gz")):
        os.system("tar xvf {} -C {}".format(path.join(root, "cifar-100-python.tar.gz"),
                                            root))

    train_images, train_labels = load_CIFAR_batch(path.join(root, "cifar-100-python"),
                                                  mode="train")
    test_images, test_labels = load_CIFAR_batch(path.join(root, "cifar-100-python"),
                                                mode="test")

    # b"fine_label_names" b"coarse_label_names"
    # meta_path = path.join(root, "cifar-100-python", "meta")
    return train_images, train_labels, test_images, test_labels

def cifa100(permute_train=False):
    """Download, parse and process cida100 data to unit scale and one-hot labels."""
    train_images, train_labels, test_images, test_labels = cifa100_raw()

    train_images = _partial_flatten(train_images) / np.float32(255.)
    test_images = _partial_flatten(test_images) / np.float32(255.)
    train_labels = _one_hot(train_labels, 100)
    test_labels = _one_hot(test_labels, 100)

    if permute_train:
        perm = np.random.RandomState(0).permutation(train_images.shape[0])
        train_images = train_images[perm]
        train_labels = train_labels[perm]

    return train_images, train_labels, test_images, test_labels


def cifa10_raw():
    """Download and parse the raw MNIST dataset."""
    base_url = "http://www.cs.toronto.edu/~kriz/"

    def load_CIFAR_batch(root, mode="train"):
        """ load single batch of cifar """
        if mode == "train":
            filenames = []
            for i in range(1,6):
                filenames.append(path.join(root, f"data_batch_{i}"))
        elif mode == "test":
            filenames = [path.join(root, "test_batch")]
        else:
            raise RuntimeError("Error: unrecognized mode",
                               " Got {}".format(mode))
        print(filenames)
        datas = []
        labels = []
        for filename in filenames:
            with open(filename, 'rb')as f:
                datadict = pickle.load(f,encoding='bytes')
                X = datadict[b'data']
                Y = datadict[b'labels']
                X = X.reshape(10000, 3, 32, 32)
                datas.append(X)
                labels.append(Y)
        return np.concatenate(datas, axis=0), np.concatenate(labels)

    for filename in ["cifar-10-python.tar.gz"]:
        _download(base_url + filename, filename, dataset_name="cifa10")

    root = path.join(_DATA, "cifa10")

    if not os.path.exists(path.join(root, "cifar-10-batches-py")):
        os.system("tar xvf {} -C {}".format(path.join(root, "cifar-10-python.tar.gz"),
                                            root))

    train_images, train_labels = load_CIFAR_batch(path.join(root, "cifar-10-batches-py"),
                                                  mode="train")
    test_images, test_labels = load_CIFAR_batch(path.join(root, "cifar-10-batches-py"),
                                                mode="test")
    print(test_images.shape)

    # b"fine_label_names" b"coarse_label_names"
    # meta_path = path.join(root, "cifar-100-python", "meta")
    return train_images, train_labels, test_images, test_labels


def cifa10(permute_train=False):
    """Download, parse and process cida100 data to unit scale and one-hot labels."""
    train_images, train_labels, test_images, test_labels = cifa10_raw()

    train_images = _partial_flatten(train_images) / np.float32(255.)
    test_images = _partial_flatten(test_images) / np.float32(255.)
    train_labels = _one_hot(train_labels, 10)
    test_labels = _one_hot(test_labels, 10)

    if permute_train:
        perm = np.random.RandomState(0).permutation(train_images.shape[0])
        train_images = train_images[perm]
        train_labels = train_labels[perm]

    return train_images, train_labels, test_images, test_labels

if __name__ == "__main__":
    train_images, train_labels, test_images, test_labels = cifa10()

    print(type(train_images), type(train_labels))
    print(train_images.shape, train_labels.shape)
    print(type(test_images), type(test_labels))
    print(test_images.shape, test_labels.shape)

    train_images, train_labels, test_images, test_labels = cifa100()

    print(type(train_images), type(train_labels))
    print(train_images.shape, train_labels.shape)
    print(type(test_images), type(test_labels))
    print(test_images.shape, test_labels.shape)

    # cifa10_filepath = path.join(_DATA, "cifa10", "cifar-10-batches-py/test_batch")
    # with open(cifa10_filepath, 'rb')as f:
    #     datadict = pickle.load(f,encoding='bytes')
    #     print(datadict.keys())
    #     print(datadict[b"data"])
    #     print(type(datadict[b"data"]))
    #     print(len(datadict[b"labels"]))