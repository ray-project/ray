from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import tempfile
import numpy as np

from ray import tune
from ray.tests.conftest import ray_start_2_cpus  # noqa: F401
from ray.experimental.sgd.tensorflow import TensorFlowTrainer, TensorFlowTrainable

from ray.experimental.sgd.tests.tf_helper import (get_model, get_dataset)


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_train(ray_start_2_cpus, num_replicas):  # noqa: F811
    trainer = TensorFlowTrainer(
        model_creator=get_model,
        data_creator=get_dataset,
        num_replicas=num_replicas,
        batch_size=512)

    train_stats1 = trainer.train()
    train_stats1.update(trainer.validate())
    print(train_stats1)

    train_stats2 = trainer.train()
    train_stats2.update(trainer.validate())
    print(train_stats2)

    print(train_stats1["train_loss"], train_stats2["train_loss"])
    print(
        train_stats1["validation_loss"],
        train_stats2["validation_loss"],
    )

    assert train_stats1["train_loss"] > train_stats2["train_loss"]
    assert train_stats1["validation_loss"] > train_stats2["validation_loss"]


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_tune_train(ray_start_2_cpus, num_replicas):  # noqa: F811

    config = {
        "model_creator": tune.function(get_model),
        "data_creator": tune.function(get_dataset),
        "num_replicas": num_replicas,
        "use_gpu": False,
        "batch_size": 512
    }

    analysis = tune.run(
        TensorFlowTrainable,
        num_samples=12,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    # checks loss decreasing for every trials
    for path, df in analysis.trial_dataframes.items():
        train_loss1 = df.loc[0, "train_loss"]
        train_loss2 = df.loc[1, "train_loss"]
        validation_loss1 = df.loc[0, "validation_loss"]
        validation_loss2 = df.loc[1, "validation_loss"]

        assert train_loss2 <= train_loss1
        assert validation_loss2 <= validation_loss1


@pytest.mark.parametrize(  # noqa: F811
    "num_replicas", [1, 2])
def test_save_and_restore(ray_start_2_cpus, num_replicas):  # noqa: F811
    trainer1 = TensorFlowTrainer(
        model_creator=get_model,
        data_creator=get_dataset,
        num_replicas=num_replicas,
        batch_size=512)
    trainer1.train()

    filename = os.path.join(tempfile.mkdtemp(), "checkpoint")
    trainer1.save(filename)

    state1 = trainer1.get_state()
    trainer1.shutdown()

    trainer2 = TensorFlowTrainer(
        model_creator=get_model,
        data_creator=get_dataset,
        num_replicas=num_replicas,
        batch_size=512)
    trainer2.restore(filename)

    state2 = trainer2.get_state()
    trainer2.shutdown()

    os.remove(filename + '.h5')
    os.remove(filename + '_state.json')

    assert set(state1.keys()) == set(state2.keys())
    for k in state1:
        if type(state1[k]) == list:
            for i in range(len(state1[k])):
                assert np.array_equal(state1[k][i], state2[k][i])
        else:
            assert state1[k] == state2[k]
