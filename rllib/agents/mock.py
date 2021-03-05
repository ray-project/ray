import os
import pickle
import numpy as np

from ray.tune import result as tune_result
from ray.rllib.agents.trainer import Trainer, with_common_config


class _MockTrainer(Trainer):
    """Mock trainer for use in tests"""

    _name = "MockTrainer"
    _default_config = with_common_config({
        "mock_error": False,
        "persistent_error": False,
        "test_variable": 1,
        "num_workers": 0,
        "user_checkpoint_freq": 0,
        "framework": "tf",
    })

    @classmethod
    def default_resource_request(cls, config):
        return None

    def _init(self, config, env_creator):
        self.info = None
        self.restored = False

    def step(self):
        if self.config["mock_error"] and self.iteration == 1 \
                and (self.config["persistent_error"] or not self.restored):
            raise Exception("mock error")
        result = dict(
            episode_reward_mean=10,
            episode_len_mean=10,
            timesteps_this_iter=10,
            info={})
        if self.config["user_checkpoint_freq"] > 0 and self.iteration > 0:
            if self.iteration % self.config["user_checkpoint_freq"] == 0:
                result.update({tune_result.SHOULD_CHECKPOINT: True})
        return result

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "mock_agent.pkl")
        with open(path, "wb") as f:
            pickle.dump(self.info, f)
        return path

    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path, "rb") as f:
            info = pickle.load(f)
        self.info = info
        self.restored = True

    def _register_if_needed(self, env_object):
        pass

    def set_info(self, info):
        self.info = info
        return info

    def get_info(self, sess=None):
        return self.info


class _SigmoidFakeData(_MockTrainer):
    """Trainer that returns sigmoid learning curves.

    This can be helpful for evaluating early stopping algorithms."""

    _name = "SigmoidFakeData"
    _default_config = with_common_config({
        "width": 100,
        "height": 100,
        "offset": 0,
        "iter_time": 10,
        "iter_timesteps": 1,
        "num_workers": 0,
    })

    def step(self):
        i = max(0, self.iteration - self.config["offset"])
        v = np.tanh(float(i) / self.config["width"])
        v *= self.config["height"]
        return dict(
            episode_reward_mean=v,
            episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={})


class _ParameterTuningTrainer(_MockTrainer):

    _name = "ParameterTuningTrainer"
    _default_config = with_common_config({
        "reward_amt": 10,
        "dummy_param": 10,
        "dummy_param2": 15,
        "iter_time": 10,
        "iter_timesteps": 1,
        "num_workers": 0,
    })

    def step(self):
        return dict(
            episode_reward_mean=self.config["reward_amt"] * self.iteration,
            episode_len_mean=self.config["reward_amt"],
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={})


def _trainer_import_failed(trace):
    """Returns dummy agent class for if PyTorch etc. is not installed."""

    class _TrainerImportFailed(Trainer):
        _name = "TrainerImportFailed"
        _default_config = with_common_config({})

        def setup(self, config):
            raise ImportError(trace)

    return _TrainerImportFailed
