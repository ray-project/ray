from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph


class PGLoss(object):
    def __init__(self, action_dist, actions, advantages):
        self.loss = -tf.reduce_mean(action_dist.logp(actions) * advantages)


class PGPolicyGraph(TFPolicyGraph):
    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.pg.pg.DEFAULT_CONFIG, **config)
        self.config = config

        # Setup policy
        obs = tf.placeholder(tf.float32, shape=[None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")
        self.model = ModelCatalog.get_model({
            "obs": obs,
            "prev_actions": prev_actions,
            "prev_rewards": prev_rewards
        }, obs_space, self.logit_dim, self.config["model"])
        action_dist = dist_class(self.model.outputs)  # logit for each action

        # Setup policy loss
        actions = ModelCatalog.get_action_placeholder(action_space)
        advantages = tf.placeholder(tf.float32, [None], name="adv")
        loss = PGLoss(action_dist, actions, advantages).loss

        # Initialize TFPolicyGraph
        sess = tf.get_default_session()
        # Mapping from sample batch keys to placeholders
        loss_in = [
            ("obs", obs),
            ("actions", actions),
            ("prev_actions", prev_actions),
            ("prev_rewards", prev_rewards),
            ("advantages", advantages),
        ]

        TFPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            sess,
            obs_input=obs,
            action_sampler=action_dist.sample(),
            loss=loss,
            loss_inputs=loss_in,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.model.seq_lens,
            max_seq_len=config["model"]["max_seq_len"])
        sess.run(tf.global_variables_initializer())

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)

    def get_initial_state(self):
        return self.model.state_init
