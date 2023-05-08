import gymnasium as gym
import numpy as np
import pytest
import ray
from ray.air import ScalingConfig

from ray.data.preprocessor import Preprocessor
from ray.rllib.algorithms import Algorithm
from ray.rllib.policy import Policy
from ray.train.rl import RLTrainer


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


scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


@pytest.mark.parametrize(
    "freq_end_expected",
    [
        (4, True, 7),  # 4, 8, 12, 16, 20, 24, 25
        (4, False, 6),  # 4, 8, 12, 16, 20, 24
        (5, True, 5),  # 5, 10, 15, 20, 25
        (0, True, 1),
        (0, False, 0),
    ],
)
def test_checkpoint_freq(ray_start_4_cpus, freq_end_expected):
    freq, end, expected = freq_end_expected

    trainer = RLTrainer(
        algorithm="__fake",
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(
                checkpoint_frequency=freq, checkpoint_at_end=end
            ),
            stop={"training_iteration": 25},
        ),
        scaling_config=scaling_config,
        config={
            "rollout_fragment_length": 1,
        },
    )
    result = trainer.fit()

    # Assert number of checkpoints
    assert len(result.best_checkpoints) == expected, str(
        [
            (metrics["training_iteration"], _cp._local_path)
            for _cp, metrics in result.best_checkpoints
        ]
    )

    # Assert checkpoint numbers are increasing
    cp_paths = [cp._local_path for cp, _ in result.best_checkpoints]
    assert cp_paths == sorted(cp_paths), str(cp_paths)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
