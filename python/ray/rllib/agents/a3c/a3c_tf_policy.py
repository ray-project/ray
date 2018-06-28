from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.catalog import ModelCatalog


class A3CLoss(object):
    def __init__(
            self, action_dist, actions, advantages, v_target, vf,
            vf_loss_coeff=0.5, entropy_coeff=-0.01):
        log_prob = action_dist.logp(actions)

        # The "policy gradients" loss
        self.pi_loss = - tf.reduce_sum(log_prob * advantages)

        delta = vf - v_target
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))
        self.entropy = tf.reduce_sum(action_dist.entropy())
        self.total_loss = (self.pi_loss +
                           self.vf_loss * vf_loss_coeff +
                           self.entropy * entropy_coeff)


class A3CPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        self.sess = tf.get_default_session()

        # Setup the policy
        self.observations = tf.placeholder(
            tf.float32, [None] + list(observation_space.shape))
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_model(
            self.observations, logit_dim, self.config["model"])
        action_dist = dist_class(self.model.outputs)
        self.vf = tf.reshape(
            linear(self.model.last_layer, 1, "value", normc_initializer(1.0)),
            [-1])
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        is_training = tf.placeholder_with_default(True, ())

        # Setup the policy loss
        if isinstance(action_space, gym.spaces.Box):
            ac_size = action_space.shape[0]
            actions = tf.placeholder(tf.float32, [None, ac_size], name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            actions = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise UnsupportedSpaceException(
                "Action space {} is not supported for A3C.".format(
                    action_space))
        advantages = tf.placeholder(tf.float32, [None], name="advantages")
        v_target = tf.placeholder(tf.float32, [None], name="v_target")
        self.loss = A3CLoss(
            action_dist, actions, advantages, v_target, self.vf,
            self.config["vf_loss_coeff"], self.config["entropy_coeff"])

        # Initialize TFPolicyGraph
        loss_in = [
            ("obs", self.observations),
            ("actions", actions),
            ("advantages", advantages),
            ("value_targets", v_target),
        ]
        for i, ph in enumerate(self.model.state_in):
            loss_in.append(("state_in_{}".format(i), ph))
        self.state_in = self.model.state_in
        self.state_out = self.model.state_out
        TFPolicyGraph.__init__(
            self, observation_space, action_space, self.sess,
            obs_input=self.observations, action_sampler=action_dist.sample(),
            loss=self.loss.total_loss, loss_inputs=loss_in,
            is_training=is_training, state_inputs=self.state_in,
            state_outputs=self.state_out,
            seq_lens=self.model.seq_lens,
            max_seq_len=self.config["model"]["max_seq_len"])

        if self.config.get("summarize"):
            bs = tf.to_float(tf.shape(self.observations)[0])
            tf.summary.scalar("model/policy_graph", self.loss.pi_loss / bs)
            tf.summary.scalar("model/value_loss", self.loss.vf_loss / bs)
            tf.summary.scalar("model/entropy", self.loss.entropy / bs)
            tf.summary.scalar("model/grad_gnorm", tf.global_norm(self._grads))
            tf.summary.scalar("model/var_gnorm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

        self.sess.run(tf.global_variables_initializer())

    def extra_compute_action_fetches(self):
        return {"vf_preds": self.vf}

    def value(self, ob, *args):
        feed_dict = {self.observations: [ob]}
        assert len(args) == len(self.state_in), (args, self.state_in)
        for k, v in zip(self.state_in, args):
            feed_dict[k] = v
        vf = self.sess.run(self.vf, feed_dict)
        return vf[0]

    def optimizer(self):
        return tf.train.AdamOptimizer(self.config["lr"])

    def gradients(self, optimizer):
        grads = tf.gradients(self.loss.total_loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_grad_fetches(self):
        if self.config.get("summarize"):
            return {"summary": self.summary_op}
        else:
            return {}

    def get_initial_state(self):
        return self.model.state_init

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            next_state = []
            for i in range(len(self.state_in)):
                next_state.append([sample_batch["state_out_{}".format(i)][-1]])
            last_r = self.value(sample_batch["new_obs"][-1], *next_state)
        return compute_advantages(
            sample_batch, last_r, self.config["gamma"], self.config["lambda"])
