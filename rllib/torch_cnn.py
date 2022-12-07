import time
import gym
import numpy as np
from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import ModelConfigDict
import torch
from torch import nn
import tensorflow as tf
from tensorflow import keras

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.models.tf.tf_modelv2 import TFModelV2

from ray.rllib.models.torch.visionnet import VisionNetwork
from ray.rllib.models.tf.visionnet import VisionNetwork as KerasVisionNetwork

config = PPOConfig()
batch_size = 1024
# feature_size = (84, 84, 1)
num_epochs = 10
device = "cpu"
env = gym.wrappers.AtariPreprocessing(gym.make("SpaceInvaders"), frame_skip=1)
# h, w, c
feature_size = env.observation_space.shape


def get_torch_data(device="cpu", unsqueeze=False):
    obs = torch.empty(batch_size, *feature_size, device=device)
    if unsqueeze:
        obs = obs.reshape(*obs.shape, 1)
    data = {
        "obs": obs,
        "actions": torch.ones(batch_size, device=device),
        "value_targets": torch.rand(batch_size, device=device),
        "advantages": torch.rand(batch_size, device=device),
        "action_logp": torch.rand(batch_size, device=device),
        "advantages": torch.rand(batch_size, device=device),
        "action_dist_inputs": torch.rand(batch_size, 6, device=device),
    }
    # data = SampleBatch(**data)
    return data


def get_tf_data():
    pass


class TorchCNN(nn.Module):
    tower_stats = {}

    def __init__(self, obs_space, action_space, num_outputs, *args, **kwargs):
        super().__init__()
        """
        self.cnn = nn.Sequential(
            nn.Conv2d(1, 32, kernel_size=8, stride=4, padding=0),
            nn.ReLU(),
            nn.Conv2d(32, 64, kernel_size=4, stride=2, padding=0),
            nn.ReLU(),
            nn.Conv2d(64, 64, kernel_size=3, stride=1, padding=0),
            nn.ReLU(),
            nn.Flatten(),
        )
        """
        self.cnn = nn.Sequential(
            nn.Conv2d(1, 16, kernel_size=8, stride=4, padding=2),
            nn.ReLU(),
            # input 21x21
            # TODO: This is different than the (1,2) padding in the actual VisionNet
            nn.Conv2d(16, 32, kernel_size=4, stride=2, padding=2),
            nn.ReLU(),
            # input 11x11
            nn.Conv2d(32, 256, kernel_size=11, stride=1, padding=0),
            nn.ReLU(),
        )
        self.logits = nn.Conv2d(256, 6, 1)
        self.vf = nn.Conv2d(256, 1, 1)
        """
        dummy = torch.from_numpy(obs_space.sample())
        dummy = dummy.reshape(1, *dummy.shape).unsqueeze(-1).permute(0, 3, 1, 2).float()
        hidden_size = self.cnn(dummy).shape[-1]
        self.mlp = nn.Sequential(
            nn.Linear(hidden_size, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Linear(128, 128),
            nn.ReLU()
        )
        self.logits = nn.Linear(128, 6)
        self.vf = nn.Linear(128, 1)
        """

    def forward(self, input_dict, state=[]):
        x = input_dict["obs"].reshape(-1, *feature_size, 1).permute(0, 3, 1, 2).float()
        x = self.cnn(x)
        logits = self.logits(x).reshape(-1, 6)
        # x = self.cnn(x)
        # x = self.mlp(x)
        # logits = self.logits(x)
        self.value = self.vf(x).reshape(-1)
        return logits, []

    def value_function(self):
        return self.value


class TorchV2NatureCNN(TorchModelV2, TorchCNN):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        TorchCNN.__init__(self, obs_space, action_space, num_outputs)

    def forward(self, input_dict, state=[], seq_lens=None):
        logits, _ = TorchCNN.forward(self, input_dict)
        return logits, []

    def value_function(self):
        return TorchCNN.value_function(self)


class TF2V2NatureCNN(TFModelV2):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        TorchCNN.__init__(self)
        self.cnn = keras.models.Sequential()
        self.cnn.add(
            keras.layers.Conv2d(
                32, 8, 4, activation="relu", input_shape=(feature_size[-1], 3)
            )
        )
        self.cnn.add(keras.layers.Conv2d(64, 4, 2, activation="relu"))
        self.cnn.add(keras.layers.Conv2d(64, 3, 1, activation="relu"))

        self.mlp = keras.models.Sequential()
        self.mlp.add(keras.layers.Dense(128, activation="relu"))
        self.mlp.add(keras.layers.Dense(128, activation="relu"))
        self.mlp.add(keras.layers.Dense(128, activation="relu"))
        self.mlp.add(keras.layers.Dense(128, activation="relu"))

        self.logits = keras.layers.Dense(6)
        self.vf = keras.layers.Dense(1)

    def forward(self, input_dict, *args, **kwargs):
        x = self.cnn(input_dict["obs"])
        x = self.mlp(x)
        logits = self.logits(x)
        self.value = tf.reshape(self.vf(x), -1)
        return logits

    def value_function(self):
        return self.value


torch_policy = PPOTorchPolicy(env.observation_space, env.action_space, PPOConfig())

torch_basemodel = TorchCNN(env.observation_space, env.action_space, 6).to(device)
torchv2_model = TorchV2NatureCNN(
    env.observation_space, env.action_space, 6, ModelConfigDict(), "bork"
).to(device)
obs_space = gym.spaces.Box(dtype=np.uint8, low=0, high=255, shape=(84, 84, 1))
# 16
# 32
# 256
torch_visionnet = VisionNetwork(
    obs_space, env.action_space, 6, ModelConfigDict(vf_share_layers=True), "bork"
).to(device)
# 21, 21, 16
# 11, 11, 32
# 1, 1, 256,
# 1, 1, 6
tf_visionnet = KerasVisionNetwork(
    obs_space, env.action_space, 6, ModelConfigDict(vf_share_layers=True), "bork"
)

torch_models = [torch_basemodel, torchv2_model, torch_visionnet]
# torch_models = []

for model in torch_models:
    print("Model:", model.__class__.__name__)
    torch_opt = torch.optim.Adam(model.parameters())
    unsqueeze = isinstance(model, VisionNetwork)
    datas = [get_torch_data(device, unsqueeze) for _ in range(num_epochs)]
    times = []
    if device != "cpu":
        torch.cuda.synchronize()
    for i in range(num_epochs):
        start = time.time()
        torch_opt.zero_grad()
        loss = torch_policy.loss(
            model=model, dist_class=TorchCategorical, train_batch=datas[i]
        )
        loss.backward()
        torch_opt.step()
        if device != "cpu":
            torch.cuda.synchronize()
        end = time.time()
        times.append(end - start)

    print(f"mean (s) {np.mean(times)}, std (s): {np.std(times)}")

print("Model (TF)", tf_visionnet.__class__.__name__)
for i in range(num_epochs):
    start = time.time()
    torch_opt.zero_grad()
    loss = tf_visionnet.loss(existing_model=model, train_batch=datas[i])
    loss.backward()
    torch_opt.step()
    if device != "cpu":
        torch.cuda.synchronize()
    end = time.time()
    times.append(end - start)
