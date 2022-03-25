# flake8: noqa

# __reproducible_start__
import numpy as np
from ray import tune


def train(config):
    # Set seed for trainable random result.
    # If you remove this line, you will get different results
    # each time you run the trial, even if the configuration
    # is the same.
    np.random.seed(config["seed"])
    random_result = np.random.uniform(0, 100, size=1).item()
    tune.report(result=random_result)


# Set seed for Ray Tune's random search.
# If you remove this line, you will get different configurations
# each time you run the script.
np.random.seed(1234)
tune.run(
    train,
    config={"seed": tune.randint(0, 1000)},
    search_alg=tune.suggest.BasicVariantGenerator(),
    num_samples=10,
)
# __reproducible_end__

# __basic_config_start__
config = {"a": {"x": tune.uniform(0, 10)}, "b": tune.choice([1, 2, 3])}
# __basic_config_end__

# __conditional_spaces_start__
config = {
    "a": tune.randint(5, 10),
    "b": tune.sample_from(lambda spec: np.random.randint(0, spec.config.a)),
}
# __conditional_spaces_end__


# __iter_start__
def _iter():
    for a in range(5, 10):
        for b in range(a):
            yield a, b


config = {
    "ab": tune.grid_search(list(_iter())),
}
# __iter_end__


def train(config):
    random_result = np.random.uniform(0, 100, size=1).item()
    tune.report(result=random_result)


train_fn = train
MOCK = True
# Note we put this check here to make sure at least the syntax of
# the code is correct. Some of these snippets simply can't be run on the nose.

if not MOCK:
    # __resources_start__
    tune.run(
        train_fn,
        resources_per_trial={"cpu": 2, "gpu": 0.5, "custom_resources": {"hdd": 80}},
    )
    # __resources_end__

    # __resources_pgf_start__
    tune.run(
        train_fn,
        resources_per_trial=tune.PlacementGroupFactory(
            [
                {"CPU": 2, "GPU": 0.5, "hdd": 80},
                {"CPU": 1},
                {"CPU": 1},
            ],
            strategy="PACK",
        ),
    )
    # __resources_pgf_end__

    metric = None

    # __modin_start__
    def train_fn(config, checkpoint_dir=None):
        # some Modin operations here
        # import modin.pandas as pd
        tune.report(metric=metric)

    tune.run(
        train_fn,
        resources_per_trial=tune.PlacementGroupFactory(
            [
                {"CPU": 1},  # this bundle will be used by the trainable itself
                {"CPU": 1},  # this bundle will be used by Modin
            ],
            strategy="PACK",
        ),
    )
# __modin_end__

# __huge_data_start__
from ray import tune
import numpy as np


def train(config, checkpoint_dir=None, num_epochs=5, data=None):
    for i in range(num_epochs):
        for sample in data:
            # ... train on sample
            pass


# Some huge dataset
data = np.random.random(size=100000000)

tune.run(tune.with_parameters(train, num_epochs=5, data=data))
# __huge_data_end__


# __seeded_1_start__
import random

random.seed(1234)
output = [random.randint(0, 100) for _ in range(10)]

# The output will always be the same.
assert output == [99, 56, 14, 0, 11, 74, 4, 85, 88, 10]
# __seeded_1_end__


# __seeded_2_start__
# This should suffice to initialize the RNGs for most Python-based libraries
import random
import numpy as np

random.seed(1234)
np.random.seed(5678)
# __seeded_2_end__


# __torch_tf_seeds_start__
import torch

torch.manual_seed(0)

import tensorflow as tf

tf.random.set_seed(0)
# __torch_tf_seeds_end__

# __torch_seed_example_start__
import random
import numpy as np
from ray import tune


def trainable(config):
    # config["seed"] is set deterministically, but differs between training runs
    random.seed(config["seed"])
    np.random.seed(config["seed"])
    # torch.manual_seed(config["seed"])
    # ... training code


config = {
    "seed": tune.randint(0, 10000),
    # ...
}

if __name__ == "__main__":
    # Set seed for the search algorithms/schedulers
    random.seed(1234)
    np.random.seed(1234)
    # Don't forget to check if the search alg has a `seed` parameter
    tune.run(trainable, config=config)
# __torch_seed_example_end__

# __large_data_start__
from ray import tune
import numpy as np


def f(config, data=None):
    pass
    # use data


data = np.random.random(size=100000000)

tune.run(tune.with_parameters(f, data=data))
# __large_data_end__

MyTrainableClass = None
custom_sync_str_or_func = ""

if not MOCK:
    # __log_1_start__
    tune.run(
        MyTrainableClass,
        local_dir="~/ray_results",
        sync_config=tune.SyncConfig(upload_dir="s3://my-log-dir"),
    )
    # __log_1_end__

    # __log_2_start__
    tune.run(
        MyTrainableClass,
        sync_config=tune.SyncConfig(
            upload_dir="s3://my-log-dir", syncer=custom_sync_str_or_func
        ),
    )
# __log_2_end__

# __sync_start__
import subprocess


def custom_sync_func(source, target):
    # run other workload here
    sync_cmd = "s3 {source} {target}".format(source=source, target=target)
    sync_process = subprocess.Popen(sync_cmd, shell=True)
    sync_process.wait()


# __sync_end__

if not MOCK:
    # __docker_start__
    from ray import tune
    from ray.tune.integration.docker import DockerSyncer

    sync_config = tune.SyncConfig(syncer=DockerSyncer)

    tune.run(train, sync_config=sync_config)
    # __docker_end__

    # __s3_start__
    from ray import tune

    tune.run(
        tune.durable(train_fn),
        # ...,
        sync_config=tune.SyncConfig(upload_dir="s3://your-s3-bucket/durable-trial/"),
    )
    # __s3_end__

    # __sync_config_start__
    from ray import tune

    tune.run(
        train_fn,
        # ...,
        local_dir="/path/to/shared/storage",
        sync_config=tune.SyncConfig(
            # Do not sync because we are on shared storage
            syncer=None
        ),
    )
    # __sync_config_end__

    # __k8s_start__
    from ray.tune.integration.kubernetes import NamespacedKubernetesSyncer

    sync_config = tune.SyncConfig(syncer=NamespacedKubernetesSyncer("ray"))

    tune.run(train, sync_config=sync_config)
# __k8s_end__

import ray

ray.shutdown()

# __local_start__
import ray

ray.init(local_mode=True)
# __local_end__

# __grid_search_start__
parameters = {
    "qux": tune.sample_from(lambda spec: 2 + 2),
    "bar": tune.grid_search([True, False]),
    "foo": tune.grid_search([1, 2, 3]),
    "baz": "asd",  # a constant value
}

tune.run(train_fn, config=parameters)
# __grid_search_end__

# __grid_search_2_start__
# num_samples=10 repeats the 3x3 grid search 10 times, for a total of 90 trials
tune.run(
    train_fn,
    name="my_trainable",
    config={
        "alpha": tune.uniform(100, 200),
        "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
        "nn_layers": [
            tune.grid_search([16, 64, 256]),
            tune.grid_search([16, 64, 256]),
        ],
    },
    num_samples=10,
)
# __grid_search_2_end__
