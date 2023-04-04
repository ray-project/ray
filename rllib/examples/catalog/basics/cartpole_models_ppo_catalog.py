"""
This file demonstrates a basic workflow that includes the PPOCatalog to build models
and an action distribution to step through an environment.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import torch

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.base import STATE_IN, ENCODER_OUT, ACTOR
from ray.rllib.policy.sample_batch import SampleBatch

env = gym.make("CartPole-v1")

catalog = PPOCatalog(env.observation_space, env.action_space, model_config_dict={})
# Build an encoder for the observation space.
encoder = catalog.build_actor_critic_encoder(framework="torch")
policy_head = catalog.build_pi_head(framework="torch")
# We expect a categorical distribution for CartPole.
action_dist_class = catalog.get_action_dist_cls(framework="torch")

# Now we are ready to interact with the environment
obs, info = env.reset()
# Encoders check for state and sequence lengths for recurrent models.
# We don't need either in this case because default encoders are not recurrent.
input_batch = {
    SampleBatch.OBS: torch.Tensor([obs]),
    STATE_IN: None,
    SampleBatch.SEQ_LENS: None,
}
# Pass everything through our models and the action distribution.
encoding = encoder(input_batch)[ENCODER_OUT][ACTOR]
action_dist_inputs = policy_head(encoding)
action_dist = action_dist_class.from_logits(action_dist_inputs)
actions = action_dist.sample().numpy()
env.step(actions[0])
# __sphinx_doc_end__
