# import re
import tempfile
from typing import Optional

import gymnasium as gym
import numpy as np

# import pandas as pd
# import pyarrow as pa
import pytest

# import ray

from ray.air.checkpoint import Checkpoint

# from ray.air.constants import MAX_REPR_LENGTH
# from ray.air.util.data_batch_conversion import (
#    convert_pandas_to_batch_type,
#    convert_batch_type_to_pandas,
# )
from ray.data.preprocessor import Preprocessor
from ray.rllib.algorithms import Algorithm
from ray.rllib.policy import Policy

# from ray.train.batch_predictor import BatchPredictor
# from ray.train.predictor import TYPE_TO_ENUM
from ray.train.rl import RLTrainer

# from ray.train.rl.rl_checkpoint import RLCheckpoint
# from ray.train.rl.rl_predictor import RLPredictor
from ray.tune.trainable.util import TrainableUtil

# from ray.train.tests.dummy_preprocessor import DummyPreprocessor


class _DummyAlgo(Algorithm):
    train_exec_impl = None

    def setup(self, config):
        random_state = config.pop("random_state", None)
        policy_class = _DummyStatefulPolicy if random_state else _DummyPolicy
        policy_config = {"random_state": random_state} if random_state else {}
        self.policy = policy_class(
            observation_space=gym.spaces.Box(low=-2.0, high=-2.0, shape=(10,)),
            action_space=gym.spaces.Discrete(n=1),
            config=policy_config,
        )

    def train(self):
        pass

    def get_policy(self, *args, **kwargs) -> Policy:
        return self.policy


class _DummyPolicy(Policy):
    """Returns actions by averaging over observations and adding a random number"""

    def compute_actions(
        self,
        obs_batch,
        *args,
        **kwargs,
    ):
        return (
            np.random.uniform(0, 1, size=len(obs_batch)) + np.mean(obs_batch, axis=1),
            [],
            {},
        )


class _DummyStatefulPolicy(Policy):
    """Returns actions by averaging over observations, adding a random state
    at initialization"""

    def __init__(self, observation_space, action_space, config):
        super().__init__(observation_space, action_space, config)
        self.random_state = config.pop("random_state")

    def compute_actions(
        self,
        obs_batch,
        *args,
        **kwargs,
    ):
        return (
            self.random_state + np.mean(obs_batch, axis=1),
            [],
            {},
        )


def create_checkpoint(
    preprocessor: Optional[Preprocessor] = None, config: Optional[dict] = None
) -> Checkpoint:
    rl_trainer = RLTrainer(
        algorithm="PPO",
        config=config or {"env": "CartPole-v1"},
        preprocessor=preprocessor,
    )
    rl_trainable_cls = rl_trainer.as_trainable()
    rl_trainable = rl_trainable_cls()

    with tempfile.TemporaryDirectory() as checkpoint_dir:
        checkpoint_file = rl_trainable.save(checkpoint_dir)
        checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint_file)
        checkpoint_data = Checkpoint.from_directory(checkpoint_path).to_dict()

    return Checkpoint.from_dict(checkpoint_data)


# def test_rl_checkpoint():
#    preprocessor = DummyPreprocessor()

#    rl_trainer = RLTrainer(
#        algorithm="PPO",
#        config={"env": "CartPole-v1"},
#        preprocessor=preprocessor,
#    )
#    rl_trainable_cls = rl_trainer.as_trainable()
#    rl_trainable = rl_trainable_cls()
#    policy = rl_trainable.get_policy()
#    predictor = RLPredictor(policy, preprocessor)

#    with tempfile.TemporaryDirectory() as checkpoint_dir:
#        checkpoint_file = rl_trainable.save(checkpoint_dir)
#        checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint_file)
#        checkpoint_data = Checkpoint.from_directory(checkpoint_path).to_dict()

#    checkpoint = RLCheckpoint.from_dict(checkpoint_data)
#    checkpoint_predictor = RLPredictor.from_checkpoint(checkpoint)

#    # Observations
#    data = pd.DataFrame([list(range(4))])
#    obs = convert_pandas_to_batch_type(data, type=TYPE_TO_ENUM[np.ndarray])

#    # Check that the policies compute the same actions
#    _ = predictor.predict(obs)
#    _ = checkpoint_predictor.predict(obs)

#    assert preprocessor == checkpoint.get_preprocessor()
#    assert checkpoint_predictor.get_preprocessor().has_preprocessed


# def test_repr():
#    checkpoint = create_checkpoint()
#    predictor = RLPredictor.from_checkpoint(checkpoint)

#    representation = repr(predictor)

#    assert len(representation) < MAX_REPR_LENGTH
#    pattern = re.compile("^RLPredictor\\((.*)\\)$")
#    assert pattern.match(representation)


# @pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
# @pytest.mark.parametrize("batch_size", [1, 20])
# def test_predict_no_preprocessor(batch_type, batch_size):
#    checkpoint = create_checkpoint()
#    predictor = RLPredictor.from_checkpoint(checkpoint)

#    # Observations
#    data = pd.DataFrame([[1.0] * 10] * batch_size)
#    obs = convert_pandas_to_batch_type(data, type=TYPE_TO_ENUM[batch_type])

#    # Predictions
#    predictions = predictor.predict(obs)
#    actions = convert_batch_type_to_pandas(predictions)

#    assert len(actions) == batch_size
#    # We add [0., 1.) to 1.0, so actions should be in [1., 2.)
#    assert all(1.0 <= action.item() < 2.0 for action in np.array(actions))


# @pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
# @pytest.mark.parametrize("batch_size", [1, 20])
# def test_predict_with_preprocessor(batch_type, batch_size):
#    preprocessor = DummyPreprocessor(lambda df: 2 * df)
#    checkpoint = create_checkpoint(preprocessor=preprocessor)
#    predictor = RLPredictor.from_checkpoint(checkpoint)

#    # Observations
#    data = pd.DataFrame([[1.0] * 10] * batch_size)
#    obs = convert_pandas_to_batch_type(data, type=TYPE_TO_ENUM[batch_type])

#    # Predictions
#    predictions = predictor.predict(obs)
#    actions = convert_batch_type_to_pandas(predictions)

#    assert len(actions) == batch_size
#    # Preprocessor doubles observations to 2.0, then we add [0., 1.),
#    # so actions should be in [2., 3.)
#    assert all(2.0 <= action.item() < 3.0 for action in np.array(actions))


# @pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table])
# @pytest.mark.parametrize("batch_size", [1, 20])
# def test_predict_batch(ray_start_4_cpus, batch_type, batch_size):
#    preprocessor = DummyPreprocessor(lambda df: 2 * df)
#    checkpoint = create_checkpoint(preprocessor=preprocessor)
#    predictor = BatchPredictor.from_checkpoint(checkpoint, RLPredictor)

#    # Observations
#    data = pd.DataFrame(
#        [[1.0] * 10] * batch_size, columns=[f"X{i:02d}" for i in range(10)]
#    )

#    if batch_type == np.ndarray:
#        dataset = ray.data.from_numpy(data.to_numpy())
#    elif batch_type == pd.DataFrame:
#        dataset = ray.data.from_pandas(data)
#    elif batch_type == pa.Table:
#        dataset = ray.data.from_arrow(pa.Table.from_pandas(data))
#    else:
#        raise RuntimeError("Invalid batch_type")

#    # Predictions
#    predictions = predictor.predict(dataset)
#    actions = predictions.to_pandas()
#    assert len(actions) == batch_size
#    # Preprocessor doubles observations to 2.0, then we add [0., 1.),
#    # so actions should be in [2., 3.)
#    assert all(2.0 <= action.item() < 3.0 for action in np.array(actions))


def test_test():
    return


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
