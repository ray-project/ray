# __doc_import_begin__
import ray
import os
import numpy as np
import tensorflow as tf
# __doc_import_end__

# initiate ray
ray.init()

# __doc_train_model_begin__
# Specify a location to save the model
TRAINED_MODEL_PATH = os.path.join("mnist_model.h5")


def train_and_save_model():
    # Load mnist dataset
    mnist = tf.keras.datasets.mnist
    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train, x_test = x_train / 255.0, x_test / 255.0

    # Train a simple neural net model
    model = tf.keras.models.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation="relu"),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(10)
    ])
    loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
    model.compile(optimizer="adam", loss=loss_fn, metrics=["accuracy"])
    model.fit(x_train, y_train, epochs=1)

    model.evaluate(x_test, y_test, verbose=2)
    model.summary()

    # Save the model in h5 format in local file system
    model.save(TRAINED_MODEL_PATH)


if not os.path.exists(TRAINED_MODEL_PATH):
    train_and_save_model()
# __doc_train_model_end__

# __doc_predict_using_model_begin__
@ray.remote
class GPUActor(object):

    def __init__(self):
        self.model = tf.keras.models.load_model("mnist_model.h5")

    def predict(self, data_input):
        return self.model.predict(data_input)

# __doc_predict_using_model_end_

# __doc_test_using_model_begin__
actor = GPUActor.remote()

your_input = np.random.randn(28 * 28).reshape((1, 28, 28))
your_result = ray.get(actor.predict.remote(your_input))

print(f"your_result is {your_result}")

ray.shutdown()
# __doc_test_using_model_end__
