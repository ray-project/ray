import gym
from gym.spaces import Discrete, Box
import numpy as np
from typing import Union, List, Dict

from ray.rllib import SampleBatch
from ray.rllib.models.torch.mingpt import (
    GPTConfig,
    GPT,
)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import (
    ModelConfigDict,
    TensorType,
    ModelInputDict,
)

torch, nn = try_import_torch()


class DTTorchModel(TorchModelV2, nn.Module):
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
        nn.Module.__init__(self)

        self.obs_dim = num_outputs

        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
        elif isinstance(action_space, Box):
            self.action_dim = np.product(action_space.shape)
        else:
            raise NotImplementedError

        # Common model parameters
        self.embed_dim = self.model_config["embed_dim"]
        self.max_seq_len = self.model_config["max_seq_len"]
        self.max_ep_len = self.model_config["max_ep_len"]
        self.block_size = self.model_config["max_seq_len"] * 3 - 1

        # Build all the nn modules
        self.transformer = self.build_transformer()
        self.position_encoder = self.build_position_encoder()
        self.action_encoder = self.build_action_encoder()
        self.obs_encoder = self.build_obs_encoder()
        self.return_encoder = self.build_return_encoder()
        self.action_head = self.build_action_head()
        self.obs_head = self.build_obs_head()
        self.return_head = self.build_return_head()

    def build_transformer(self):
        # build the model
        gpt_config = GPTConfig(
            block_size=self.block_size,
            n_layer=self.model_config["num_layers"],
            n_head=self.model_config["num_heads"],
            n_embed=self.embed_dim,
            embed_pdrop=self.model_config["embed_pdrop"],
            resid_pdrop=self.model_config["resid_pdrop"],
            attn_pdrop=self.model_config["attn_pdrop"],
        )
        gpt = GPT(gpt_config)
        return gpt

    def build_position_encoder(self):
        return nn.Embedding(self.max_ep_len, self.embed_dim)

    def build_action_encoder(self):
        if isinstance(self.action_space, Discrete):
            return nn.Embedding(self.action_dim, self.embed_dim)
        elif isinstance(self.action_space, Box):
            return nn.Linear(self.action_dim, self.embed_dim)
        else:
            raise NotImplementedError

    def build_obs_encoder(self):
        return nn.Linear(self.obs_dim, self.embed_dim)

    def build_return_encoder(self):
        return nn.Linear(self.embed_dim, 1)

    def build_action_head(self):
        return nn.Linear(self.embed_dim, self.action_dim)

    def build_obs_head(self):
        if not self.model_config["use_obs_output"]:
            return None
        return nn.Linear(self.embed_dim, self.obs_dim)

    def build_return_head(self):
        if not self.model_config["use_return_output"]:
            return None
        return nn.Linear(self.embed_dim, 1)

    def get_prediction(
        self,
        model_out: TensorType,
        input_dict: SampleBatch,
    ) -> Dict[str, TensorType]:
        """ """
        pass

    def get_target(self, input_dict: SampleBatch) -> Dict[str, TensorType]:
        """ """
        pass
