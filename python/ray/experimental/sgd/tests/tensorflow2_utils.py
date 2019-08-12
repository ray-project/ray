from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch
import torch.nn as nn
import torch.utils.data



def model_creator(config):
    input_shape = (HEIGHT, WIDTH, NUM_CHANNELS)
    img_input = tf.keras.layers.Input(shape=input_shape)
    model = resnet.resnet56(img_input=img_input, classes=NUM_CLASSES)

    return model

def optimizer_creator(model, config):
    opt = keras.optimizers.SGD(learning_rate=0.1, momentum=0.9)
    loss = 'sparse_categorical_crossentropy'
    metrics=['sparse_categorical_accuracy']

    return opt, loss, metrics

def data_creator(config):
    """Returns training set, validation set"""
    (x,y), (x_test, y_test) = keras.datasets.cifar10.load_data()

    train_dataset = tf.data.Dataset.from_tensor_slices((x,y))
    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))

    tf.random.set_seed(22)
    train_dataset = train_dataset.map(_augmentation).map(_normalize).shuffle(NUM_TRAIN_SAMPLES).batch(BS_PER_GPU * NUM_GPUS, drop_remainder=True)
    test_dataset = test_dataset.map(normalize).batch(BS_PER_GPU * NUM_GPUS, drop_remainder=True)

    # fixes TypeError
    train_dataset = train_dataset.map(_cast_to_float32)
    test_dataset = test_dataset.map(_cast_to_float32)

    return train_dataset, test_dataset


def _normalize(x, y):
  x = tf.image.per_image_standardization(x)
  return x, y

def _augmentation(x, y):
    x = tf.image.resize_with_crop_or_pad(
        x, HEIGHT + 8, WIDTH + 8)
    x = tf.image.random_crop(x, [HEIGHT, WIDTH, NUM_CHANNELS])
    x = tf.image.random_flip_left_right(x)
    return x, y

def _cast_to_float32(x, y):
  x = tf.cast(x, tf.float32)
  return x, y


# TODO: need simpler model

# class LinearDataset(torch.utils.data.Dataset):
#     """y = a * x + b"""

#     def __init__(self, a, b, size=1000):
#         x = np.random.random(size).astype(np.float32) * 10
#         x = np.arange(0, 10, 10 / size, dtype=np.float32)
#         self.x = torch.from_numpy(x)
#         self.y = torch.from_numpy(a * x + b)

#     def __getitem__(self, index):
#         return self.x[index, None], self.y[index, None]

#     def __len__(self):
#         return len(self.x)


# def model_creator(config):
#     return nn.Linear(1, 1)


# def optimizer_creator(model, config):
#     """Returns criterion, optimizer"""
#     criterion = nn.MSELoss()
#     optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)
#     return criterion, optimizer


# def data_creator(config):
#     """Returns training set, validation set"""
#     return LinearDataset(2, 5), LinearDataset(2, 5, size=400)
