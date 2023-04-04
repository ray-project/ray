"""
This files demonstrates basic interaction with Catalogs in RLlib.
"""


# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

env = gym.make("CartPole-v1")

catalog = PPOCatalog(env.observation_space, env.action_space, model_config_dict={})
# Build an encoder that fits CartPole's observation space.
encoder = catalog.build_actor_critic_encoder(framework="torch")
policy_head = catalog.build_pi_head(framework="torch")
# We expect a categorical distribution for CartPole.
action_dist_class = catalog.get_action_dist_cls(framework="torch")
# __sphinx_doc_end__
