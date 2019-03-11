from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.annotations import override


class PGLoss(object):
    """Simple policy gradient loss."""

    def __init__(self, action_dist, actions, advantages):
        self.loss = -tf.reduce_mean(action_dist.logp(actions) * advantages)


class PGPolicyGraph(TFPolicyGraph):
    """Simple policy gradient example of defining a policy graph."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.pg.pg.DEFAULT_CONFIG, **config)
        self.config = config

        # Setup placeholders
        obs = tf.placeholder(tf.float32, shape=[None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")

        # Create the model network and action outputs
        self.model = ModelCatalog.get_model({
            "obs": obs,
            "prev_actions": prev_actions,
            "prev_rewards": prev_rewards,
            "is_training": self._get_is_training_placeholder(),
        }, obs_space, action_space, self.logit_dim, self.config["model"])
        action_dist = dist_class(self.model.outputs)  # logit for each action

        # Setup policy loss
        actions = ModelCatalog.get_action_placeholder(action_space)
        advantages = tf.placeholder(tf.float32, [None], name="adv")
        loss = PGLoss(action_dist, actions, advantages).loss

        # Mapping from sample batch keys to placeholders. These keys will be
        # read from postprocessed sample batches and fed into the specified
        # placeholders during loss computation.
        loss_in = [
            ("obs", obs),
            ("actions", actions),
            ("prev_actions", prev_actions),
            ("prev_rewards", prev_rewards),
            ("advantages", advantages),  # added during postprocessing
        ]

        # Initialize TFPolicyGraph
        sess = tf.get_default_session()
        TFPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            sess,
            obs_input=obs,
            action_sampler=action_dist.sample(),
            action_prob=action_dist.sampled_action_prob(),
            loss=loss,
            loss_inputs=loss_in,
            model=self.model,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.model.seq_lens,
            max_seq_len=config["model"]["max_seq_len"])
        sess.run(tf.global_variables_initializer())

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        # This adds the "advantages" column to the sample batch
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)

    @override(PolicyGraph)
    def get_initial_state(self):
        return self.model.state_init

    @override(TFPolicyGraph)
    def optimizer(self):
        return tf.train.AdamOptimizer(learning_rate=self.config["lr"])
