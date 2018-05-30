from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.a3c.a3c_tf_policy import A3CTFPolicyGraph
from ray.rllib.models.catalog import ModelCatalog


class SharedModel(A3CTFPolicyGraph):

    def __init__(self, ob_space, ac_space, registry, config, **kwargs):
        super(SharedModel, self).__init__(
            ob_space, ac_space, registry, config, **kwargs)

    def _setup_graph(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.get_model(
            self.registry, self.x, self.logit_dim, self.config["model"])
        self.logits = self._model.outputs
        self.action_dist = dist_class(self.logits)
        self.vf = tf.reshape(linear(self._model.last_layer, 1, "value",
                                    normc_initializer(1.0)), [-1])

        self.sample = self.action_dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

        self.state_in = []
        self.state_out = []

    def setup_loss(self, action_space):
        A3CTFPolicyGraph.setup_loss(self, action_space)
        self.loss_in = [
            ("obs", self.x),
            ("actions", self.ac),
            ("advantages", self.adv),
            ("value_targets", self.r),
        ]

    def extra_compute_action_fetches(self):
        return {"vf_preds": self.vf}

    def value(self, ob, *args):
        vf = self.sess.run(self.vf, {self.x: [ob]})
        return vf[0]
