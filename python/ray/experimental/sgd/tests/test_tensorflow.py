from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import tempfile
import numpy as np
import shutil

from ray import tune
from ray.tests.conftest import ray_start_2_cpus  # noqa: F401
from ray.experimental.sgd.tf import TFTrainer, TFTrainable

from ray.experimental.sgd.examples.tensorflow_train_example import (
    simple_model, simple_dataset)

SIMPLE_CONFIG = {
    "batch_size": 128,
    "fit_config": {
        "steps_per_epoch": 3,
    },
    "evaluate_config": {
        "steps": 3,
    }
}


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_train(ray_start_2_cpus, num_replicas):  # noqa: F811
    trainer = TFTrainer(
        model_creator=simple_model,
        data_creator=simple_dataset,
        num_replicas=num_replicas,
        config=SIMPLE_CONFIG)

    train_stats1 = trainer.train()
    train_stats1.update(trainer.validate())

    train_stats2 = trainer.train()
    train_stats2.update(trainer.validate())


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_tune_train(ray_start_2_cpus, num_replicas):  # noqa: F811

    config = {
        "model_creator": tune.function(simple_model),
        "data_creator": tune.function(simple_dataset),
        "num_replicas": num_replicas,
        "use_gpu": False,
        "trainer_config": SIMPLE_CONFIG
    }

    tune.run(
        TFTrainable,
        num_samples=2,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_save_and_restore(ray_start_2_cpus, num_replicas):  # noqa: F811
    trainer1 = TFTrainer(
        model_creator=simple_model,
        data_creator=simple_dataset,
        num_replicas=num_replicas,
        config=SIMPLE_CONFIG)
    trainer1.train()

    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, "checkpoint")
    trainer1.save(filename)

    model1 = trainer1.get_model()
    trainer1.shutdown()

    trainer2 = TFTrainer(
        model_creator=simple_model,
        data_creator=simple_dataset,
        num_replicas=num_replicas,
        config=SIMPLE_CONFIG)
    trainer2.restore(filename)

    model2 = trainer2.get_model()
    trainer2.shutdown()

    shutil.rmtree(tmpdir)

    model1_config = model1.get_config()
    model2_config = model2.get_config()
    assert _compare(model1_config, model2_config, skip_keys=["name"])

    model1_weights = model1.get_weights()
    model2_weights = model2.get_weights()
    assert _compare(model1_weights, model2_weights)

    model1_opt_weights = model1.optimizer.get_weights()
    model2_opt_weights = model2.optimizer.get_weights()
    assert _compare(model1_opt_weights, model2_opt_weights)


def _compare(d1, d2, skip_keys=None):
    """Compare two lists or dictionaries or array"""
    if type(d1) != type(d2):
        return False

    if isinstance(d1, dict):
        if set(d1) != set(d2):
            return False

        for key in d1:
            if skip_keys is not None and key in skip_keys:
                continue

            if not _compare(d1[key], d2[key], skip_keys=skip_keys):
                return False

    elif isinstance(d1, list):
        for i, _ in enumerate(d1):
            if not _compare(d1[i], d2[i], skip_keys=skip_keys):
                return False

    elif isinstance(d1, np.ndarray):
        if not np.array_equal(d1, d2):
            return False
    else:
        if d1 != d2:
            return False

    return True
