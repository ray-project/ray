"""
This file demonstrates a basic workflow that includes the Catalog base class and
RLlib's ModelConfigs to build models and an action distribution to step through an
environment.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import torch

from ray.rllib.core.models.base import STATE_IN, ENCODER_OUT
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.policy.sample_batch import SampleBatch

env = gym.make("CartPole-v1")

catalog = Catalog(env.observation_space, env.action_space, model_config_dict={})
# We expect a categorical distribution for CartPole.
action_dist_class = catalog.get_action_dist_cls(framework="torch")
#  Therefore, we need env.action_space.n action distribution inputs.
expected_action_dist_input_dims = (env.action_space.n,)
# Build an encoder for the observation space.
encoder = catalog.build_encoder(framework="torch")
# Build a suitable head model for the action distribution.
head_config = MLPHeadConfig(input_dims=catalog.latent_dims,
                          output_dims=expected_action_dist_input_dims)
head = head_config.build(framework="torch")

# Now we are ready to interact with the environment
obs, info = env.reset()
# Encoders check for state and sequence lengths for recurrent models.
# We don't need either in this case because default encoders are not recurrent.
input_batch = {
    SampleBatch.OBS: torch.Tensor([obs]),
    STATE_IN: None,
    SampleBatch.SEQ_LENS: None
}
# Pass everything through our models and the action distribution.
encoding = encoder(input_batch)[ENCODER_OUT]
action_dist_inputs = head(encoding)
action_dist = action_dist_class.from_logits(action_dist_inputs)
actions = action_dist.sample().numpy()
env.step(actions[0])
# __sphinx_doc_end__
