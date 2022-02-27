# flake8: noqa
"""
This file holds code for the TF best-practices guide in the documentation.

FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
formatting on this module and update the paragraph below. See issue #21318.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""

# fmt: off
# __tf_model_start__


def create_keras_model():
    from tensorflow import keras
    from tensorflow.keras import layers
    model = keras.Sequential()
    # Adds a densely-connected layer with 64 units to the model:
    model.add(layers.Dense(64, activation="relu", input_shape=(32, )))
    # Add another:
    model.add(layers.Dense(64, activation="relu"))
    # Add a softmax layer with 10 output units:
    model.add(layers.Dense(10, activation="softmax"))

    model.compile(
        optimizer=keras.optimizers.RMSprop(0.01),
        loss=keras.losses.categorical_crossentropy,
        metrics=[keras.metrics.categorical_accuracy])
    return model
# __tf_model_end__
# fmt: on

# fmt: off
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
class Network(object):
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
# fmt: on

# fmt: off
# __actor_start__
NetworkActor = Network.remote()
result_object_ref = NetworkActor.train.remote()
ray.get(result_object_ref)
# __actor_end__
# fmt: on

# fmt: off
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
