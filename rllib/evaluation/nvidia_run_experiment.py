import tempfile
from torch import nn
from torch.nn import Sequential as Seq, Linear, LeakyReLU, Sigmoid, Softmax,ReLU
from pprint import pprint
import ray
import os
from ray import tune
from ray.rllib.agents import ppo
from ray.rllib.env.env_context import EnvContext
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.tune.logger import UnifiedLogger
import torch as th
from ray.tune.registry import register_env
from ray.tune.logger import pretty_print
from ray.rllib.evaluation import MultiAgentEpisode, RolloutWorker
from ray.rllib.env import BaseEnv
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.policy import Policy
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.modelv2 import restore_original_dimensions

import json
import time

import numpy as np
from gym.spaces.discrete import Discrete
import gym
from gym import spaces


class MyEnv(gym.Env):
    """Custom Environment that follows gym interface"""
    def __init__(self) :
        super(MyEnv, self).__init__()
        self.steps = 0
        self.features = np.random.rand(2* 128 * 128) # 128 KB
        self.observation_space = gym.spaces.Dict(
            {"features": spaces.Box(
                low = -100,
                high = 100,
                shape=self.features.shape,
                dtype=np.float32)
            })
        self.action_space = spaces.Discrete(2)
        self.max_steps = 130
        # self.max_steps = 1
        """
        the env obs has 128 x 128 + 68 * 130 = 25224 floats per step,
         each episode has 130 steps, and there are 256 episode
         collected per iteration. So roughly 3.2GB data to process
         at iteration.
        """

    def step(self, action: gym.Space):
        # start = time.time()
        # print("Steps",self.steps,action)
        self.features = np.random.rand(2 * 128 * 128)
        self.steps += 1
        if self.steps < self.max_steps:
            done = False
        else :
            done = True
        reward = 0
        observation = { 'features': self.features }
        info = {}
        # print(f">>>>> MyEnv.step() took {(time.time() - start)*1000}ms")
        return observation, reward, done, info


    def reset(self) -> np.array:
        self.steps = 0
        return {"features": self.features}

    def render(self, mode='human'):
        pass
    def close (self):
        pass

class CustomTorchModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                                              model_config, name)
        nn.Module.__init__(self)

        self.policy_net = Seq(
            Linear(2*128*128,2),
            ReLU()
            )
        self.value_net = Seq(
            Linear(2*128*128,1),
            ReLU()
            )

    def forward(self, input_dict, state, seq_lens):
        obs = input_dict["obs_flat"].float()
        new_obs = restore_original_dimensions(obs,self.obs_space,"torch")
        features = new_obs["features"]
        # print(next(self.policy_net.parameters()).is_cuda)
        policy = self.policy_net(features)
        self._value_out = th.flatten(self.value_net(features))
        return policy, []

    def value_function(self):
        return self._value_out


def train():

        ray.init(dashboard_port=8080, object_store_memory=40 * 1024* 1024 * 1024)

        register_env("my_env", lambda config: MyEnv())

        ModelCatalog.register_custom_model("my_torch_model", CustomTorchModel)

        config = ppo.DEFAULT_CONFIG.copy()
        # config["num_workers"]= 64
        config["num_workers"]= 32
        # config["num_cpus_per_worker"] = 1
        config["num_gpus_per_worker"] = 0.125
        config["num_envs_per_worker"] = 1
        config["num_gpus"] = 1
        config["num_cpus_for_driver"] = 16
        config["vf_loss_coeff"] = 0.5
        config["kl_target"] = 0.01
        config["entropy_coeff"] = 0.01
        config["clip_param"] = 0.2
        config["framework"] = "torch"
        config["seed"] = 0
        config["lr"] = 0.0004
        config["sgd_minibatch_size"] = 16
        config["gamma"] = 1
        config["rollout_fragment_length"] = 130
        # config["rollout_fragment_length"] = 1
        config["train_batch_size"] = 130 * 32 * 4
        # config["train_batch_size"] = 4 * 4
        config["num_sgd_iter"] = 4
        config["model"] = {
            "vf_share_layers": True,
            "custom_model": "my_torch_model",
            "custom_model_config": {}
        }
        # config["local_dir"] = "/".join([".",cfg.arch.name,"seed_",str(cfg.seed)])
        pprint(config)
        config["env_config"] = {}
        trainer = ppo.PPOTrainer(env = "my_env",
                         config=config,
                         logger_creator = custom_log_creator())

        for k in range(1):
                result = trainer.train()
                print(pretty_print(result))


def custom_log_creator():

    # timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    logdir_path = "./logs"

    def logger_creator(config):

        if not os.path.exists(logdir_path):
            os.makedirs(logdir_path)
        logdir = tempfile.mkdtemp(prefix="tensorboard_",dir=logdir_path)
        return UnifiedLogger(config, logdir, loggers=None)

    return logger_creator
if __name__ == "__main__":
        train()