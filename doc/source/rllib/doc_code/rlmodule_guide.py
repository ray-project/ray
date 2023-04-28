# flake8: noqa
from ray.rllib.utils.annotations import override
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.models.specs.specs_base import TensorSpec


# __enabling-rlmodules-in-configs-begin__
import torch
from pprint import pprint

from ray.rllib.algorithms.ppo import PPOConfig

config = (
    PPOConfig()
    .framework("torch")
    .environment("CartPole-v1")
    .rl_module(_enable_rl_module_api=True)
)

algorithm = config.build()

# run for 2 training steps
for _ in range(2):
    result = algorithm.train()
    pprint(result)
# __enabling-rlmodules-in-configs-end__


# __constructing-rlmodules-sa-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

env = gym.make("CartPole-v1")

spec = SingleAgentRLModuleSpec(
    module_class=DiscreteBCTorchModule,
    observation_space=env.observation_space,
    action_space=env.action_space,
    model_config_dict={"fcnet_hiddens": [64]},
)

module = spec.build()
# __constructing-rlmodules-sa-end__


# __constructing-rlmodules-ma-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

spec = MultiAgentRLModuleSpec(
    module_specs={
        "module_1": SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=gym.spaces.Box(low=-1, high=1, shape=(10,)),
            action_space=gym.spaces.Discrete(2),
            model_config_dict={"fcnet_hiddens": [32]},
        ),
        "module_2": SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=gym.spaces.Box(low=-1, high=1, shape=(5,)),
            action_space=gym.spaces.Discrete(2),
            model_config_dict={"fcnet_hiddens": [16]},
        ),
    },
)

marl_module = spec.build()
# __constructing-rlmodules-ma-end__


# __pass-specs-to-configs-sa-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.bc_algorithm import BCConfigTest


config = (
    BCConfigTest()
    .environment("CartPole-v1")
    .rl_module(
        _enable_rl_module_api=True,
        rl_module_spec=SingleAgentRLModuleSpec(module_class=DiscreteBCTorchModule),
    )
    .training(model={"fcnet_hiddens": [32, 32]})
)

algo = config.build()
# __pass-specs-to-configs-sa-end__


# __pass-specs-to-configs-ma-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.bc_algorithm import BCConfigTest
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole


config = (
    BCConfigTest()
    .environment(MultiAgentCartPole, env_config={"num_agents": 2})
    .rl_module(
        _enable_rl_module_api=True,
        rl_module_spec=MultiAgentRLModuleSpec(
            module_specs=SingleAgentRLModuleSpec(module_class=DiscreteBCTorchModule)
        ),
    )
    .training(model={"fcnet_hiddens": [32, 32]})
)
# __pass-specs-to-configs-ma-end__


# __convert-sa-to-ma-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

env = gym.make("CartPole-v1")
spec = SingleAgentRLModuleSpec(
    module_class=DiscreteBCTorchModule,
    observation_space=env.observation_space,
    action_space=env.action_space,
    model_config_dict={"fcnet_hiddens": [64]},
)

module = spec.build()
marl_module = module.as_multi_agent()
# __convert-sa-to-ma-end__


# __write-custom-sa-rlmodule-torch-begin__
from typing import Mapping, Any
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.utils.nested_dict import NestedDict

import torch
import torch.nn as nn


class DiscreteBCTorchModule(TorchRLModule):
    def __init__(self, config: RLModuleConfig) -> None:
        super().__init__(config)

    def setup(self):
        input_dim = self.config.observation_space.shape[0]
        hidden_dim = self.config.model_config_dict["fcnet_hiddens"][0]
        output_dim = self.config.action_space.n

        self.policy = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

        self.input_dim = input_dim

    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        action_logits = self.policy(batch["obs"])
        return {"action_dist": torch.distributions.Categorical(logits=action_logits)}


# __write-custom-sa-rlmodule-torch-end__


# __write-custom-sa-rlmodule-tf-begin__
from typing import Mapping, Any
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.utils.nested_dict import NestedDict

import tensorflow as tf


class DiscreteBCTfModule(TfRLModule):
    def __init__(self, config: RLModuleConfig) -> None:
        super().__init__(config)

    def setup(self):
        input_dim = self.config.observation_space.shape[0]
        hidden_dim = self.config.model_config_dict["fcnet_hiddens"][0]
        output_dim = self.config.action_space.n

        self.policy = tf.keras.Sequential(
            [
                tf.keras.layers.Dense(hidden_dim, activation="relu"),
                tf.keras.layers.Dense(output_dim),
            ]
        )

        self.input_dim = input_dim

    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        return self._forward_train(batch)

    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        return self._forward_train(batch)

    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        action_logits = self.policy(batch["obs"])
        return {"action_dist": tf.distributions.Categorical(logits=action_logits)}


# __write-custom-sa-rlmodule-tf-end__


# __extend-spec-checking-single-level-begin__
class DiscreteBCTorchModule(TorchRLModule):
    ...

    @override(TorchRLModule)
    def input_specs_exploration(self) -> SpecType:
        # Enforce that input nested dict to exploration method has a key "obs"
        return ["obs"]

    @override(TorchRLModule)
    def output_specs_exploration(self) -> SpecType:
        # Enforce that output nested dict from exploration method has a key
        # "action_dist"
        return ["action_dist"]


# __extend-spec-checking-single-level-end__


# __extend-spec-checking-nested-begin__
class DiscreteBCTorchModule(TorchRLModule):
    ...

    @override(TorchRLModule)
    def input_specs_exploration(self) -> SpecType:
        # Enforce that input nested dict to exploration method has a key "obs"
        # and within that key, it has a key "global" and "local". There should
        # also be a key "action_mask"
        return [("obs", "global"), ("obs", "local"), "action_mask"]


# __extend-spec-checking-nested-end__


# __extend-spec-checking-torch-specs-begin__
class DiscreteBCTorchModule(TorchRLModule):
    ...

    @override(TorchRLModule)
    def input_specs_exploration(self) -> SpecType:
        # Enforce that input nested dict to exploration method has a key "obs"
        # and its value is a torch.Tensor with shape (b, h) where b is the
        # batch size (determined at run-time) and h is the hidden size
        # (fixed at 10).
        return {"obs": TensorSpec("b, h", h=10, framework="torch")}


# __extend-spec-checking-torch-specs-end__


# __extend-spec-checking-type-specs-begin__
class DiscreteBCTorchModule(TorchRLModule):
    ...

    @override(TorchRLModule)
    def output_specs_exploration(self) -> SpecType:
        # Enforce that output nested dict from exploration method has a key
        # "action_dist" and its value is a torch.distribution.Categorical
        return {"action_dist": torch.distribution.Categorical}


# __extend-spec-checking-type-specs-end__


# __write-custom-marlmodule-shared-enc-begin__
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModuleConfig,
    MultiAgentRLModule,
)
from ray.rllib.utils.nested_dict import NestedDict

import torch
import torch.nn as nn


class BCTorchRLModuleWithSharedGlobalEncoder(TorchRLModule):
    """An RLModule with a shared encoder between agents for global observation."""

    def __init__(
        self, encoder: nn.Module, local_dim: int, hidden_dim: int, action_dim: int
    ) -> None:
        super().__init__(config=None)

        self.encoder = encoder
        self.policy_head = nn.Sequential(
            nn.Linear(hidden_dim + local_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, action_dim),
        )

    def _forward_inference(self, batch):
        with torch.no_grad():
            return self._common_forward(batch)

    def _forward_exploration(self, batch):
        with torch.no_grad():
            return self._common_forward(batch)

    def _forward_train(self, batch):
        return self._common_forward(batch)

    def _common_forward(self, batch):
        obs = batch["obs"]
        global_enc = self.encoder(obs["global"])
        policy_in = torch.cat([global_enc, obs["local"]], dim=-1)
        action_logits = self.policy_head(policy_in)

        return {"action_dist": torch.distributions.Categorical(logits=action_logits)}


class BCTorchMultiAgentModuleWithSharedEncoder(MultiAgentRLModule):
    def __init__(self, config: MultiAgentRLModuleConfig) -> None:
        super().__init__(config)

    def setup(self):

        module_specs = self.config.modules
        module_spec = next(iter(module_specs.values()))
        global_dim = module_spec.observation_space["global"].shape[0]
        hidden_dim = module_spec.model_config_dict["fcnet_hiddens"][0]
        shared_encoder = nn.Sequential(
            nn.Linear(global_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
        )

        rl_modules = {}
        for module_id, module_spec in module_specs.items():
            rl_modules[module_id] = BCTorchRLModuleWithSharedGlobalEncoder(
                encoder=shared_encoder,
                local_dim=module_spec.observation_space["local"].shape[0],
                hidden_dim=hidden_dim,
                action_dim=module_spec.action_space.n,
            )

        self._rl_modules = rl_modules


# __write-custom-marlmodule-shared-enc-end__


# __pass-custom-marlmodule-shared-enc-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec

spec = MultiAgentRLModuleSpec(
    marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
    module_specs={
        "local_2d": SingleAgentRLModuleSpec(
            observation_space=gym.spaces.Dict(
                {
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                }
            ),
            action_space=gym.spaces.Discrete(2),
            model_config_dict={"fcnet_hiddens": [64]},
        ),
        "local_5d": SingleAgentRLModuleSpec(
            observation_space=gym.spaces.Dict(
                {
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(5,)),
                }
            ),
            action_space=gym.spaces.Discrete(5),
            model_config_dict={"fcnet_hiddens": [64]},
        ),
    },
)

module = spec.build()
# __pass-custom-marlmodule-shared-enc-end__
