"""CIFAR dataset input module, with the majority taken from
https://github.com/tensorflow/models/tree/master/resnet.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf


def build_data(data_path, size, dataset):
    """Creates the queue and preprocessing operations for the dataset.

    Args:
        data_path: Filename for cifar10 data.
        size: The number of images in the dataset.
        dataset: The dataset we are using.

    Returns:
        queue: A Tensorflow queue for extracting the images and labels.
    """
    image_size = 32
    if dataset == "cifar10":
        label_bytes = 1
        label_offset = 0
    elif dataset == "cifar100":
        label_bytes = 1
        label_offset = 1
    depth = 3
    image_bytes = image_size * image_size * depth
    record_bytes = label_bytes + label_offset + image_bytes

    def load_transform(value):
        # Convert these examples to dense labels and processed images.
        record = tf.reshape(tf.decode_raw(value, tf.uint8), [record_bytes])
        label = tf.cast(
            tf.slice(record, [label_offset], [label_bytes]), tf.int32)
        # Convert from string to [depth * height * width] to
        # [depth, height, width].
        depth_major = tf.reshape(
            tf.slice(record, [label_bytes], [image_bytes]),
            [depth, image_size, image_size])
        # Convert from [depth, height, width] to [height, width, depth].
        image = tf.cast(tf.transpose(depth_major, [1, 2, 0]), tf.float32)
        return (image, label)

    # Read examples from files in the filename queue.
    data_files = tf.gfile.Glob(data_path)
    data = tf.data.FixedLengthRecordDataset(
        data_files, record_bytes=record_bytes)
    data = data.map(load_transform)
    data = data.batch(size)
    iterator = data.make_one_shot_iterator()
    return iterator.get_next()


def build_input(data, batch_size, dataset, train):
    """Build CIFAR image and labels.

    Args:
        data_path: Filename for cifar10 data.
        batch_size: Input batch size.
        train: True if we are training and false if we are testing.

    Returns:
        images: Batches of images of size
            [batch_size, image_size, image_size, 3].
        labels: Batches of labels of size [batch_size, num_classes].

    Raises:
      ValueError: When the specified dataset is not supported.
    """
    image_size = 32
    depth = 3
    num_classes = 10 if dataset == "cifar10" else 100
    images, labels = data
    num_samples = images.shape[0] - images.shape[0] % batch_size
    dataset = tf.data.Dataset.from_tensor_slices(
        (images[:num_samples], labels[:num_samples]))

    def map_train(image, label):
        image = tf.image.resize_image_with_crop_or_pad(image, image_size + 4,
                                                       image_size + 4)
        image = tf.random_crop(image, [image_size, image_size, 3])
        image = tf.image.random_flip_left_right(image)
        image = tf.image.per_image_standardization(image)
        return (image, label)

    def map_test(image, label):
        image = tf.image.resize_image_with_crop_or_pad(image, image_size,
                                                       image_size)
        image = tf.image.per_image_standardization(image)
        return (image, label)

    dataset = dataset.map(map_train if train else map_test)
    dataset = dataset.batch(batch_size)
    dataset = dataset.repeat()
    if train:
        dataset = dataset.shuffle(buffer_size=16 * batch_size)
    images, labels = dataset.make_one_shot_iterator().get_next()
    images = tf.reshape(images, [batch_size, image_size, image_size, depth])
    labels = tf.reshape(labels, [batch_size, 1])
    indices = tf.reshape(tf.range(0, batch_size, 1), [batch_size, 1])
    labels = tf.sparse_to_dense(
        tf.concat([indices, labels], 1), [batch_size, num_classes], 1.0, 0.0)

    assert len(images.get_shape()) == 4
    assert images.get_shape()[0] == batch_size
    assert images.get_shape()[-1] == 3
    assert len(labels.get_shape()) == 2
    assert labels.get_shape()[0] == batch_size
    assert labels.get_shape()[1] == num_classes
    if not train:
        tf.summary.image("images", images)
    return images, labels
