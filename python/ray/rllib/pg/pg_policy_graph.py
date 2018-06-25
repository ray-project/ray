from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.postprocessing import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


class PGLoss(object):
    def __init__(self, action_dist, actions, advantages):
        self.loss = -tf.reduce_mean(action_dist.logp(actions) * advantages)


class PGPolicyGraph(TFPolicyGraph):

    def __init__(self, obs_space, action_space, config):
        self.config = config

        # Setup policy
        self.x = tf.placeholder(
            tf.float32, shape=[None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_model(
            self.x, self.logit_dim, options=self.config["model"])
        self.dist = dist_class(self.model.outputs)  # logit for each action

        # Setup policy loss
        self.ac = ModelCatalog.get_action_placeholder(action_space)
        self.adv = tf.placeholder(tf.float32, [None], name="adv")
        self.loss = PGLoss(self.dist, self.ac, self.adv).loss

        # Initialize TFPolicyGraph
        self.sess = tf.get_default_session()
        self.loss_in = [
            ("obs", self.x),
            ("actions", self.ac),
            ("advantages", self.adv),
        ]

        # LSTM support
        for i, ph in enumerate(self.model.state_in):
            self.loss_in.append(("state_in_{}".format(i), ph))

        self.is_training = tf.placeholder_with_default(True, ())
        TFPolicyGraph.__init__(
            self, self.sess, obs_input=self.x,
            action_sampler=self.dist.sample(), loss=self.loss,
            loss_inputs=self.loss_in, is_training=self.is_training,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            seq_lens=self.model.seq_lens,
            max_seq_len=config["model"]["max_seq_len"])
        self.sess.run(tf.global_variables_initializer())

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return compute_advantages(
            sample_batch, 0.0, self.config["gamma"], use_gae=False)

    def get_initial_state(self):
        return self.model.state_init
