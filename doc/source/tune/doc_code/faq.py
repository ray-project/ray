# flake8: noqa

# __reproducible_start__
import numpy as np
from ray import tune
from ray.air import session, ScalingConfig


def train(config):
    # Set seed for trainable random result.
    # If you remove this line, you will get different results
    # each time you run the trial, even if the configuration
    # is the same.
    np.random.seed(config["seed"])
    random_result = np.random.uniform(0, 100, size=1).item()
    session.report({"result": random_result})


# Set seed for Ray Tune's random search.
# If you remove this line, you will get different configurations
# each time you run the script.
np.random.seed(1234)
tuner = tune.Tuner(
    train,
    tune_config=tune.TuneConfig(
        num_samples=10,
        search_alg=tune.search.BasicVariantGenerator(),
    ),
    param_space={"seed": tune.randint(0, 1000)},
)
tuner.fit()
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
    session.report({"result": random_result})


train_fn = train
MOCK = True
# Note we put this check here to make sure at least the syntax of
# the code is correct. Some of these snippets simply can't be run on the nose.

if not MOCK:
    # __resources_start__
    tuner = tune.Tuner(
        tune.with_resources(
            train_fn, resources={"cpu": 2, "gpu": 0.5, "custom_resources": {"hdd": 80}}
        ),
    )
    tuner.fit()
    # __resources_end__

    # __resources_pgf_start__
    tuner = tune.Tuner(
        tune.with_resources(
            train_fn,
            resources=tune.PlacementGroupFactory(
                [
                    {"CPU": 2, "GPU": 0.5, "hdd": 80},
                    {"CPU": 1},
                    {"CPU": 1},
                ],
                strategy="PACK",
            ),
        )
    )
    tuner.fit()
    # __resources_pgf_end__

    # __resources_scalingconfig_start__
    tuner = tune.Tuner(
        tune.with_resources(
            train_fn,
            resources=ScalingConfig(
                trainer_resources={"CPU": 2, "GPU": 0.5, "hdd": 80},
                num_workers=2,
                resources_per_worker={"CPU": 1},
            ),
        )
    )
    tuner.fit()
    # __resources_scalingconfig_end__

    # __resources_lambda_start__
    tuner = tune.Tuner(
        tune.with_resources(
            train_fn,
            resources=lambda config: {"GPU": 1} if config["use_gpu"] else {"GPU": 0},
        ),
        param_space={
            "use_gpu": True,
        },
    )
    tuner.fit()
    # __resources_lambda_end__

    metric = None

    # __modin_start__
    def train_fn(config, checkpoint_dir=None):
        # some Modin operations here
        # import modin.pandas as pd
        session.report({"metric": metric})

    tuner = tune.Tuner(
        tune.with_resources(
            train_fn,
            resources=tune.PlacementGroupFactory(
                [
                    {"CPU": 1},  # this bundle will be used by the trainable itself
                    {"CPU": 1},  # this bundle will be used by Modin
                ],
                strategy="PACK",
            ),
        )
    )
    tuner.fit()
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

tuner = tune.Tuner(tune.with_parameters(train, num_epochs=5, data=data))
tuner.fit()
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
    tuner = tune.Tuner(trainable, param_space=config)
    tuner.fit()
# __torch_seed_example_end__

# __large_data_start__
from ray import tune, air
import numpy as np


def f(config, data=None):
    pass
    # use data


data = np.random.random(size=100000000)

tuner = tune.Tuner(tune.with_parameters(f, data=data))
tuner.fit()
# __large_data_end__

MyTrainableClass = None

if not MOCK:
    # __log_1_start__
    tuner = tune.Tuner(
        MyTrainableClass,
        sync_config=tune.SyncConfig(upload_dir="s3://my-log-dir"),
    )
    tuner.fit()
    # __log_1_end__

    # __log_2_start__
    from ray.tune.syncer import Syncer

    class CustomSyncer(Syncer):
        def sync_up(
            self, local_dir: str, remote_dir: str, exclude: list = None
        ) -> bool:
            pass  # sync up

        def sync_down(
            self, remote_dir: str, local_dir: str, exclude: list = None
        ) -> bool:
            pass  # sync down

        def delete(self, remote_dir: str) -> bool:
            pass  # delete

    tuner = tune.Tuner(
        MyTrainableClass,
        sync_config=tune.SyncConfig(
            upload_dir="s3://my-log-dir", syncer=CustomSyncer()
        ),
    )
    tuner.fit()
    # __log_2_end__

    # __custom_command_syncer_start__
    import subprocess
    from ray.tune.syncer import Syncer

    class CustomCommandSyncer(Syncer):
        def __init__(
            self,
            sync_up_template: str,
            sync_down_template: str,
            delete_template: str,
            sync_period: float = 300.0,
        ):
            self.sync_up_template = sync_up_template
            self.sync_down_template = sync_down_template
            self.delete_template = delete_template

            super().__init__(sync_period=sync_period)

        def sync_up(
            self, local_dir: str, remote_dir: str, exclude: list = None
        ) -> bool:
            cmd_str = self.sync_up_template.format(
                source=local_dir,
                target=remote_dir,
            )
            try:
                subprocess.check_call(cmd_str, shell=True)
            except Exception as e:
                print(f"Exception when syncing up {local_dir} to {remote_dir}: {e}")
                return False
            return True

        def sync_down(
            self, remote_dir: str, local_dir: str, exclude: list = None
        ) -> bool:
            cmd_str = self.sync_down_template.format(
                source=remote_dir,
                target=local_dir,
            )
            try:
                subprocess.check_call(cmd_str, shell=True)
            except Exception as e:
                print(f"Exception when syncing down {remote_dir} to {local_dir}: {e}")
                return False
            return True

        def delete(self, remote_dir: str) -> bool:
            cmd_str = self.delete_template.format(
                target=remote_dir,
            )
            try:
                subprocess.check_call(cmd_str, shell=True)
            except Exception as e:
                print(f"Exception when deleting {remote_dir}: {e}")
                return False
            return True

        def retry(self):
            raise NotImplementedError

        def wait(self):
            pass

    sync_config = tune.SyncConfig(
        syncer=CustomCommandSyncer(
            sync_up_template="aws s3 sync {source} {target}",
            sync_down_template="aws s3 sync {source} {target}",
            delete_template="aws s3 rm {target} --recursive",
        ),
        upload_dir="s3://bucket/path",
    )
    # __custom_command_syncer_end__


if not MOCK:
    # __s3_start__
    from ray import tune

    tuner = tune.Tuner(
        train_fn,
        # ...,
        sync_config=tune.SyncConfig(upload_dir="s3://your-s3-bucket/durable-trial/"),
    )
    tuner.fit()
    # __s3_end__

    # __sync_config_start__
    from ray import air, tune

    tuner = tune.Tuner(
        train_fn,
        run_config=air.RunConfig(
            local_dir="/path/to/shared/storage",
        ),
        sync_config=tune.SyncConfig(
            # Do not sync because we are on shared storage
            syncer=None
        ),
    )
    tuner.fit()
    # __sync_config_end__


import ray

ray.shutdown()

# __grid_search_start__
parameters = {
    "qux": tune.sample_from(lambda spec: 2 + 2),
    "bar": tune.grid_search([True, False]),
    "foo": tune.grid_search([1, 2, 3]),
    "baz": "asd",  # a constant value
}

tuner = tune.Tuner(train_fn, param_space=parameters)
tuner.fit()
# __grid_search_end__

# __grid_search_2_start__
# num_samples=10 repeats the 3x3 grid search 10 times, for a total of 90 trials
tuner = tune.Tuner(
    train_fn,
    run_config=air.RunConfig(
        name="my_trainable",
    ),
    param_space={
        "alpha": tune.uniform(100, 200),
        "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
        "nn_layers": [
            tune.grid_search([16, 64, 256]),
            tune.grid_search([16, 64, 256]),
        ],
    },
    tune_config=tune.TuneConfig(
        num_samples=10,
    ),
)
# __grid_search_2_end__

if not MOCK:
    import os
    from pathlib import Path

    # __no_chdir_start__
    def train_func(config):
        # Read from relative paths
        print(open("./read.txt").read())

        # The working directory shouldn't have changed from the original
        # NOTE: The `TUNE_ORIG_WORKING_DIR` environment variable is deprecated.
        assert os.getcwd() == os.environ["TUNE_ORIG_WORKING_DIR"]

        # Write to the Tune trial directory, not the shared working dir
        tune_trial_dir = Path(session.get_trial_dir())
        with open(tune_trial_dir / "write.txt", "w") as f:
            f.write("trial saved artifact")

    tuner = tune.Tuner(
        train_func,
        tune_config=tune.TuneConfig(..., chdir_to_trial_dir=False),
    )
    tuner.fit()
    # __no_chdir_end__
