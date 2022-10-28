import os
import pickle
import numpy as np

from ray.tune import result as tune_result
from ray.rllib.algorithms.algorithm import Algorithm, with_common_config
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AlgorithmConfigDict


class _MockTrainer(Algorithm):
    """Mock trainer for use in tests"""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return with_common_config(
            {
                "mock_error": False,
                "persistent_error": False,
                "test_variable": 1,
                "num_workers": 0,
                "user_checkpoint_freq": 0,
                "framework": "tf",
            }
        )

    @classmethod
    def default_resource_request(cls, config):
        return None

    @override(Algorithm)
    def setup(self, config):
        # Setup our config: Merge the user-supplied config (which could
        # be a partial config dict with the class' default).
        self.config = self.merge_trainer_configs(
            self.get_default_config(), config, True
        )
        self.config["env"] = self._env_id

        self.callbacks = self.config["callbacks"]()

        # Add needed properties.
        self.info = None
        self.restored = False

    @override(Algorithm)
    def step(self):
        if (
            self.config["mock_error"]
            and self.iteration == 1
            and (self.config["persistent_error"] or not self.restored)
        ):
            raise Exception("mock error")
        result = dict(
            episode_reward_mean=10, episode_len_mean=10, timesteps_this_iter=10, info={}
        )
        if self.config["user_checkpoint_freq"] > 0 and self.iteration > 0:
            if self.iteration % self.config["user_checkpoint_freq"] == 0:
                result.update({tune_result.SHOULD_CHECKPOINT: True})
        return result

    @override(Algorithm)
    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "mock_agent.pkl")
        with open(path, "wb") as f:
            pickle.dump(self.info, f)
        return path

    @override(Algorithm)
    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path, "rb") as f:
            info = pickle.load(f)
        self.info = info
        self.restored = True

    @staticmethod
    @override(Algorithm)
    def _get_env_id_and_creator(env_specifier, config):
        # No env to register.
        return None, None

    def set_info(self, info):
        self.info = info
        return info

    def get_info(self, sess=None):
        return self.info


class _SigmoidFakeData(_MockTrainer):
    """Trainer that returns sigmoid learning curves.

    This can be helpful for evaluating early stopping algorithms."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return with_common_config(
            {
                "width": 100,
                "height": 100,
                "offset": 0,
                "iter_time": 10,
                "iter_timesteps": 1,
                "num_workers": 0,
            }
        )

    def step(self):
        i = max(0, self.iteration - self.config["offset"])
        v = np.tanh(float(i) / self.config["width"])
        v *= self.config["height"]
        return dict(
            episode_reward_mean=v,
            episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={},
        )


class _ParameterTuningTrainer(_MockTrainer):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return with_common_config(
            {
                "reward_amt": 10,
                "dummy_param": 10,
                "dummy_param2": 15,
                "iter_time": 10,
                "iter_timesteps": 1,
                "num_workers": 0,
            }
        )

    def step(self):
        return dict(
            episode_reward_mean=self.config["reward_amt"] * self.iteration,
            episode_len_mean=self.config["reward_amt"],
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={},
        )


def _algorithm_import_failed(trace):
    """Returns dummy Algorithm class for if PyTorch etc. is not installed."""

    class _TrainerImportFailed(Algorithm):
        _name = "TrainerImportFailed"

        def setup(self, config):
            raise ImportError(trace)

    return _TrainerImportFailed
