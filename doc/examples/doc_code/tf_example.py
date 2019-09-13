# flake8: noqa
"""
This file holds code for the TF best-practices guide in the documentation.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# yapf: disable
# __tf_model_start__
import tensorflow as tf
from tensorflow.keras import layers


def create_keras_model():
    model = tf.keras.Sequential()
    # Adds a densely-connected layer with 64 units to the model:
    model.add(layers.Dense(64, activation="relu", input_shape=(32, )))
    # Add another:
    model.add(layers.Dense(64, activation="relu"))
    # Add a softmax layer with 10 output units:
    model.add(layers.Dense(10, activation="softmax"))

    model.compile(
        optimizer=tf.train.RMSPropOptimizer(0.01),
        loss=tf.keras.losses.categorical_crossentropy,
        metrics=[tf.keras.metrics.categorical_accuracy])
    return model
# __tf_model_end__
# yapf: enable

# yapf: disable
# __ray_start__
import ray
import numpy as np

ray.init()

def random_one_hot_labels(shape):
    n, n_class = shape
    classes = np.random.randint(0, n_class, n)
    labels = np.zeros((n, n_class))
    labels[np.arange(n), classes] = 1
    return labels


# Use GPU wth
# @ray.remote(num_gpus=1)
@ray.remote
class Network():
    def __init__(self):
        self.model = create_keras_model()
        self.dataset = np.random.random((1000, 32))
        self.labels = random_one_hot_labels((1000, 10))

    def train(self):
        history = self.model.fit(self.dataset, self.labels, verbose=False)
        return history.history

    def get_weights(self):
        return self.model.get_weights()

    def set_weights(self, weights):
        # Note that for simplicity this does not handle the optimizer state.
        self.model.set_weights(weights)
# __ray_end__
# yapf: enable

# yapf: disable
# __actor_start__
NetworkActor = Network.remote()
result_object_id = NetworkActor.train.remote()
ray.get(result_object_id)
# __actor_end__
# yapf: enable

# yapf: disable
# __weight_average_start__
NetworkActor2 = Network.remote()
NetworkActor2.train.remote()
weights = ray.get(
    [NetworkActor.get_weights.remote(),
     NetworkActor2.get_weights.remote()])

averaged_weights = [(layer1 + layer2) / 2
                    for layer1, layer2 in zip(weights[0], weights[1])]

weight_id = ray.put(averaged_weights)
[
    actor.set_weights.remote(weight_id)
    for actor in [NetworkActor, NetworkActor2]
]
ray.get([actor.train.remote() for actor in [NetworkActor, NetworkActor2]])
