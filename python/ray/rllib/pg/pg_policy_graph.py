from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.postprocessing import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


class PGLoss(object):
    def __init__(self, action_dist, actions, advantages):
        self.loss = -tf.reduce_mean(action_dist.logp(actions) * advantages)


class PGPolicyGraph(TFPolicyGraph):
    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.pg.pg.DEFAULT_CONFIG, **config)
        self.config = config

        # Setup policy
        obs = tf.placeholder(tf.float32, shape=[None]+list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        model = ModelCatalog.get_model(
            obs, self.logit_dim, options=self.config["model"])
        action_dist = dist_class(model.outputs)  # logit for each action

        # Setup policy loss
        actions = ModelCatalog.get_action_placeholder(action_space)
        advantages = tf.placeholder(tf.float32, [None], name="adv")
        loss = PGLoss(action_dist, actions, advantages).loss

        # Initialize TFPolicyGraph
        sess = tf.get_default_session()
        loss_in = [
            ("obs", obs),
            ("actions", actions),
            ("advantages", advantages),
        ]
        self.is_training = tf.placeholder_with_default(True, ())
        TFPolicyGraph.__init__(
            self, obs_space, action_space, sess, obs_input=obs,
            action_sampler=action_dist.sample(), loss=loss,
            loss_inputs=loss_in, is_training=self.is_training)
        sess.run(tf.global_variables_initializer())

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)
