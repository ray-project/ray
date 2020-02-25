import os
import json
from filelock import FileLock

import tensorflow as tf
tf.keras.backend.set_image_data_format("channels_last")

MultiWorkerMirroredStrategy = tf.distribute.experimental.MultiWorkerMirroredStrategy
Dataset = tf.data.Dataset
from tensorflow.keras import utils


import ray

from tensorflow.keras.datasets import cifar10
def fetch_keras_data():
    with FileLock(os.path.expanduser("~/.cifar.lock")):
        (x_train, y_train), (x_test, y_test) = cifar10.load_data()

    x_train = x_train.astype("float32") / 255
    y_train = utils.to_categorical(y_train, 10)

    return (x_train, y_train)

def create_dataset():
    dat = fetch_keras_data()

    res = Dataset.from_tensor_slices(dat)
    res = res.batch(32) # todo: shuffle

    return res


from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Flatten, Activation
from tensorflow.keras.optimizers import RMSprop
from tensorflow.keras.losses import CategoricalCrossentropy
def create_model():
    model = Sequential()
    model.add(Flatten(input_shape=(32, 32, 3)))
    model.add(Dense(10))
    model.add(Activation("softmax"))

    opt = RMSprop()
    loss = CategoricalCrossentropy()
    model.compile(loss=loss, optimizer=opt)

    return model

import socket
from contextlib import closing
def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

class Runner:
    def __init__(self, rank):
        self.rank = rank

    def setup(self, urls):
        os.environ["TF_CONFIG"] = json.dumps({
            "cluster": {
                "worker": urls
            },
            "task": {
                "index": self.rank,
                "type": "worker"
            }
        })

        self.strat = MultiWorkerMirroredStrategy()
        with self.strat.scope():
            self.data = create_dataset()
            self.model = create_model()

    def fit(self):
        return self.model.fit(
            self.data,
            verbose=self.rank == 0,
            steps_per_epoch=50000/32).history

    def get_node_ip(self):
        return ray.services.get_node_ip_address()

    def find_free_port(self):
        return find_free_port()

ray.init(address='auto')

RRunner = ray.remote(num_cpus=1, num_gpus=0)(Runner)
runners = [
    RRunner.remote(i) for i in range(4)
]

ips = ray.get([r.get_node_ip.remote() for r in runners])
ports = ray.get([r.find_free_port.remote() for r in runners])

urls = [
    "{ip}:{port}".format(ip=ips[i], port=ports[i])
    for i in range(len(runners))
]
ray.get([r.setup.remote(urls) for r in runners])

for i in range(100):
    logs = ray.get([r.fit.remote() for r in runners])
    print(logs)
