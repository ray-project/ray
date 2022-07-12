import tempfile
from typing import Optional

import gym
import numpy as np
import pandas as pd
import pytest

from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.rllib.algorithms import Algorithm
from ray.rllib.policy import Policy
from ray.train.rl import RLTrainer
from ray.train.rl.rl_predictor import RLPredictor
from ray.tune.trainable.util import TrainableUtil


class _DummyAlgo(Algorithm):
    train_exec_impl = None

    def setup(self, config):
        self.policy = _DummyPolicy(
            observation_space=gym.spaces.Box(low=-2.0, high=-2.0, shape=(10,)),
            action_space=gym.spaces.Discrete(n=1),
            config={},
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


class _DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df * 2


def create_checkpoint(
    preprocessor: Optional[Preprocessor] = None, config: Optional[dict] = None
) -> Checkpoint:
    rl_trainer = RLTrainer(
        algorithm=_DummyAlgo,
        config=config or {},
        preprocessor=preprocessor,
    )
    rl_trainable_cls = rl_trainer.as_trainable()
    rl_trainable = rl_trainable_cls()

    with tempfile.TemporaryDirectory() as checkpoint_dir:
        checkpoint_file = rl_trainable.save(checkpoint_dir)
        checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint_file)
        checkpoint_data = Checkpoint.from_directory(checkpoint_path).to_dict()

    return Checkpoint.from_dict(checkpoint_data)


@pytest.mark.parametrize("batch_type", [np.array, pd.DataFrame])
@pytest.mark.parametrize("batch_size", [1, 20])
def test_predict_no_preprocessor(batch_type, batch_size):
    checkpoint = create_checkpoint()
    predictor = RLPredictor.from_checkpoint(checkpoint)

    # Observations
    obs = batch_type([[1.0] * 10] * batch_size)
    actions = predictor.predict(obs)

    assert len(actions) == batch_size
    # We add [0., 1.) to 1.0, so actions should be in [1., 2.)
    assert all(1.0 <= action.item() < 2.0 for action in np.array(actions))


@pytest.mark.parametrize("batch_type", [np.array, pd.DataFrame])
@pytest.mark.parametrize("batch_size", [1, 20])
def test_predict_with_preprocessor(batch_type, batch_size):
    preprocessor = _DummyPreprocessor()
    checkpoint = create_checkpoint(preprocessor=preprocessor)
    predictor = RLPredictor.from_checkpoint(checkpoint)

    # Observations
    obs = batch_type([[1.0] * 10] * batch_size)
    actions = predictor.predict(obs)

    assert len(actions) == batch_size
    # Preprocessor doubles observations to 2.0, then we add [0., 1.),
    # so actions should be in [2., 3.)
    assert all(2.0 <= action.item() < 3.0 for action in np.array(actions))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
