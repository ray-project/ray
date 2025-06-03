"""
This file implements a self-attention Encoder (see https://arxiv.org/abs/1909.07528) for handling variable-length Repeated observation spaces. It expects a Dict observation space with Discrete, Box, or Repeated (of Discrete or Box) subspaces.

This is an example of using custom encoders to efficiently process structured
"""
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.base import Encoder, ENCODER_OUT
from ray.rllib.core.models.configs import ModelConfig
from gymnasium.spaces import Discrete, Box, Dict
from ray.rllib.utils.spaces.repeated import Repeated
import torch
from torch import nn

from ray.rllib.env.wrappers.repeated_wrapper import ObsVectorizationWrapper

class AttentionEncoder(TorchModel, Encoder):
    '''
      An Encoder that takes a Dict of multiple spaces, including Discrete, Box, and Repeated, and uses an attention layer to convert this variable-length input into a fixed-length featurized learned representation.
    '''
    def __init__(self, config):
        super().__init__(config)
        self.observation_space = config.observation_space.original
        self.emb_dim = config.emb_dim
        # Use an attention layer to reduce observations to a fixed length
        self.mha = nn.MultiheadAttention(self.emb_dim, 4, batch_first=True)
        self.residual = nn.Linear(self.emb_dim, self.emb_dim)
        # We expect a dict of Boxes, Discretes, or Repeateds composed of same.
        embs = {}
        for n, s in self.observation_space.spaces.items():
          if (type(s)==Repeated):
            s = s.child_space # embed layer applies to child space
          if (type(s)==Box):
            embs[n] = nn.Linear(s.shape[0], self.emb_dim)
          elif (type(s)==Discrete):
            embs[n] = nn.Embedding(s.n, self.emb_dim)
          else:
            raise Exception("Unsupported observation subspace")
        self.embs = nn.ModuleDict(embs)

    def _forward(self, input_dict, **kwargs):
        N = input_dict['obs'].shape[0]
        vec = input_dict['obs']
        # The original space we mapped from.
        obs_s = self.observation_space
        obs = ObsVectorizationWrapper.restore_obs_batch(vec, obs_s)
        embeddings = []
        masks = []
        for s in sorted(obs.keys()):
          v = obs[s]
          if type(obs_s[s]) == Repeated:
            mask = v[1]
            v = torch.stack(v[0]).permute(1,0,2) # seq_len, batch_size, unit_size
          elif type(obs_s[s]) in [Box, Discrete]:
            mask = torch.ones((N,1)) # Fixed elements are always there
            v = v.unsqueeze(1)
          embedded = self.embs[s](v)
          embeddings.append(embedded)
          masks.append(mask)
        # All entities have embeddings. Apply masked residual self-attention and then mean-pool.
        x = torch.concatenate(embeddings,dim=1) # batch_size, seq_len, unit_size
        mask = torch.concatenate(masks, dim=1)  # batch_size, seq_len
        # Attention
        x_attn, _ = self.mha(x, x, x,key_padding_mask=mask,need_weights=False)
        x = self.residual(x_attn) + x
        # Masked mean-pooling.
        mask = mask.unsqueeze(dim=2)
        x = x * mask  # Mask x to exclude nonexistent entries from mean pool op
        x = x.mean(dim=1) * mask.shape[1] / mask.sum(dim=1) # Adjust mean
        return {ENCODER_OUT: x}

class AttentionEncoderConfig(ModelConfig):
    '''
      Produces an AttentionEncoder.

      kwargs:
       * attention_emb_dim: The embedding dimension of the attention layer.
    '''
    def __init__(self, observation_space, **kwargs):
        self.observation_space = observation_space
        self.emb_dim = kwargs['model_config_dict']['attention_emb_dim']
        self.output_dims = (self.emb_dim,)
    def build(self, framework):
        return AttentionEncoder(self)
    def output_dims(self):
      return self.output_dims