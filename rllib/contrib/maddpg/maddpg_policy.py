from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.agents.dqn.dqn_policy import minimize_and_clip, _adjust_nstep
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import try_import_tf

import logging
from gym.spaces import Box, Discrete
import numpy as np

logger = logging.getLogger(__name__)

tf = try_import_tf()


class MADDPGPostprocessing(object):
    """Implements agentwise termination signal and n-step learning."""

    @override(Policy)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        # FIXME: Get done from info is required since agentwise done is not
        # supported now.
        sample_batch.data["dones"] = self.get_done_from_info(
            sample_batch.data["infos"])

        # N-step Q adjustments
        if self.config["n_step"] > 1:
            _adjust_nstep(self.config["n_step"], self.config["gamma"],
                          sample_batch[SampleBatch.CUR_OBS],
                          sample_batch[SampleBatch.ACTIONS],
                          sample_batch[SampleBatch.REWARDS],
                          sample_batch[SampleBatch.NEXT_OBS],
                          sample_batch[SampleBatch.DONES])

        return sample_batch


class MADDPGTFPolicy(MADDPGPostprocessing, TFPolicy):
    def __init__(self, obs_space, act_space, config):
        # _____ Initial Configuration
        self.config = config = dict(ray.rllib.contrib.maddpg.DEFAULT_CONFIG,
                                    **config)
        self.global_step = tf.train.get_or_create_global_step()

        # FIXME: Get done from info is required since agentwise done is not
        # supported now.
        self.get_done_from_info = np.vectorize(
            lambda info: info.get("done", False))

        agent_id = config["agent_id"]
        if agent_id is None:
            raise ValueError("Must set `agent_id` in the policy config.")
        if type(agent_id) is not int:
            raise ValueError("Agent ids must be integers for MADDPG.")

        # _____ Environment Setting
        def _make_continuous_space(space):
            if isinstance(space, Box):
                return space
            elif isinstance(space, Discrete):
                return Box(
                    low=np.zeros((space.n, )), high=np.ones((space.n, )))
            else:
                raise UnsupportedSpaceException(
                    "Space {} is not supported.".format(space))

        obs_space_n = [
            _make_continuous_space(space)
            for _, (_, space, _,
                    _) in sorted(config["multiagent"]["policies"].items())
        ]
        act_space_n = [
            _make_continuous_space(space)
            for _, (_, _, space,
                    _) in sorted(config["multiagent"]["policies"].items())
        ]

        # _____ Placeholders
        # Placeholders for policy evaluation and updates
        def _make_ph_n(space_n, name=""):
            return [
                tf.placeholder(
                    tf.float32,
                    shape=(None, ) + space.shape,
                    name=name + "_%d" % i) for i, space in enumerate(space_n)
            ]

        obs_ph_n = _make_ph_n(obs_space_n, "obs")
        act_ph_n = _make_ph_n(act_space_n, "actions")
        new_obs_ph_n = _make_ph_n(obs_space_n, "new_obs")
        new_act_ph_n = _make_ph_n(act_space_n, "new_actions")
        rew_ph = tf.placeholder(
            tf.float32, shape=None, name="rewards_{}".format(agent_id))
        done_ph = tf.placeholder(
            tf.float32, shape=None, name="dones_{}".format(agent_id))

        if config["use_local_critic"]:
            obs_space_n, act_space_n = [obs_space_n[agent_id]], [
                act_space_n[agent_id]
            ]
            obs_ph_n, act_ph_n = [obs_ph_n[agent_id]], [act_ph_n[agent_id]]
            new_obs_ph_n, new_act_ph_n = [new_obs_ph_n[agent_id]], [
                new_act_ph_n[agent_id]
            ]
            agent_id = 0

        # _____ Value Network
        # Build critic network for t.
        critic, _, critic_model_n, critic_vars = self._build_critic_network(
            obs_ph_n,
            act_ph_n,
            obs_space_n,
            act_space_n,
            hiddens=config["critic_hiddens"],
            activation=getattr(tf.nn, config["critic_hidden_activation"]),
            scope="critic")

        # Build critic network for t + 1.
        target_critic, _, _, target_critic_vars = self._build_critic_network(
            new_obs_ph_n,
            new_act_ph_n,
            obs_space_n,
            act_space_n,
            hiddens=config["critic_hiddens"],
            activation=getattr(tf.nn, config["critic_hidden_activation"]),
            scope="target_critic")

        # Build critic loss.
        td_error = tf.subtract(
            tf.stop_gradient(
                rew_ph + (1.0 - done_ph) *
                (config["gamma"]**config["n_step"]) * target_critic[:, 0]),
            critic[:, 0])
        critic_loss = tf.reduce_mean(td_error**2)

        # _____ Policy Network
        # Build actor network for t.
        act_sampler, actor_feature, actor_model, actor_vars = (
            self._build_actor_network(
                obs_ph_n[agent_id],
                obs_space_n[agent_id],
                act_space_n[agent_id],
                hiddens=config["actor_hiddens"],
                activation=getattr(tf.nn, config["actor_hidden_activation"]),
                scope="actor"))

        # Build actor network for t + 1.
        self.new_obs_ph = new_obs_ph_n[agent_id]
        self.target_act_sampler, _, _, target_actor_vars = (
            self._build_actor_network(
                self.new_obs_ph,
                obs_space_n[agent_id],
                act_space_n[agent_id],
                hiddens=config["actor_hiddens"],
                activation=getattr(tf.nn, config["actor_hidden_activation"]),
                scope="target_actor"))

        # Build actor loss.
        act_n = act_ph_n.copy()
        act_n[agent_id] = act_sampler
        critic, _, _, _ = self._build_critic_network(
            obs_ph_n,
            act_n,
            obs_space_n,
            act_space_n,
            hiddens=config["critic_hiddens"],
            activation=getattr(tf.nn, config["critic_hidden_activation"]),
            scope="critic")
        actor_loss = -tf.reduce_mean(critic)
        if config["actor_feature_reg"] is not None:
            actor_loss += config["actor_feature_reg"] * tf.reduce_mean(
                actor_feature**2)

        # _____ Losses
        self.losses = {"critic": critic_loss, "actor": actor_loss}

        # _____ Optimizers
        self.optimizers = {
            "critic": tf.train.AdamOptimizer(config["critic_lr"]),
            "actor": tf.train.AdamOptimizer(config["actor_lr"])
        }

        # _____ Build variable update ops.
        self.tau = tf.placeholder_with_default(
            config["tau"], shape=(), name="tau")

        def _make_target_update_op(vs, target_vs, tau):
            return [
                target_v.assign(tau * v + (1.0 - tau) * target_v)
                for v, target_v in zip(vs, target_vs)
            ]

        self.update_target_vars = _make_target_update_op(
            critic_vars + actor_vars, target_critic_vars + target_actor_vars,
            self.tau)

        def _make_set_weight_op(variables):
            vs = list()
            for v in variables.values():
                vs += v
            phs = [
                tf.placeholder(
                    tf.float32,
                    shape=v.get_shape(),
                    name=v.name.split(":")[0] + "_ph") for v in vs
            ]
            return tf.group(*[v.assign(ph) for v, ph in zip(vs, phs)]), phs

        self.vars = {
            "critic": critic_vars,
            "actor": actor_vars,
            "target_critic": target_critic_vars,
            "target_actor": target_actor_vars
        }
        self.update_vars, self.vars_ph = _make_set_weight_op(self.vars)

        # _____ TensorFlow Initialization

        self.sess = tf.get_default_session()

        def _make_loss_inputs(placeholders):
            return [(ph.name.split("/")[-1].split(":")[0], ph)
                    for ph in placeholders]

        loss_inputs = _make_loss_inputs(obs_ph_n + act_ph_n + new_obs_ph_n +
                                        new_act_ph_n + [rew_ph, done_ph])

        TFPolicy.__init__(
            self,
            obs_space,
            act_space,
            self.sess,
            obs_input=obs_ph_n[agent_id],
            action_sampler=act_sampler,
            loss=actor_loss + critic_loss,
            loss_inputs=loss_inputs)

        self.sess.run(tf.global_variables_initializer())

        # Hard initial update
        self.update_target(1.0)

    @override(TFPolicy)
    def optimizer(self):
        return None

    @override(TFPolicy)
    def gradients(self, optimizer, loss):
        if self.config["grad_norm_clipping"] is not None:
            self.gvs = {
                k: minimize_and_clip(optimizer, self.losses[k], self.vars[k],
                                     self.config["grad_norm_clipping"])
                for k, optimizer in self.optimizers.items()
            }
        else:
            self.gvs = {
                k: optimizer.compute_gradients(self.losses[k], self.vars[k])
                for k, optimizer in self.optimizers.items()
            }
        return self.gvs["critic"] + self.gvs["actor"]

    @override(TFPolicy)
    def build_apply_op(self, optimizer, grads_and_vars):
        critic_apply_op = self.optimizers["critic"].apply_gradients(
            self.gvs["critic"])

        with tf.control_dependencies([tf.assign_add(self.global_step, 1)]):
            with tf.control_dependencies([critic_apply_op]):
                actor_apply_op = self.optimizers["actor"].apply_gradients(
                    self.gvs["actor"])

        return actor_apply_op

    @override(TFPolicy)
    def extra_compute_action_feed_dict(self):
        return {}

    @override(TFPolicy)
    def extra_compute_grad_fetches(self):
        return {LEARNER_STATS_KEY: {}}

    @override(TFPolicy)
    def get_weights(self):
        var_list = []
        for var in self.vars.values():
            var_list += var
        return self.sess.run(var_list)

    @override(TFPolicy)
    def set_weights(self, weights):
        self.sess.run(
            self.update_vars, feed_dict=dict(zip(self.vars_ph, weights)))

    @override(Policy)
    def get_state(self):
        return TFPolicy.get_state(self)

    @override(Policy)
    def set_state(self, state):
        TFPolicy.set_state(self, state)

    def _build_critic_network(self,
                              obs_n,
                              act_n,
                              obs_space_n,
                              act_space_n,
                              hiddens,
                              activation=None,
                              scope=None):
        with tf.variable_scope(scope, reuse=tf.AUTO_REUSE) as scope:
            if self.config["use_state_preprocessor"]:
                model_n = [
                    ModelCatalog.get_model({
                        "obs": obs,
                        "is_training": self._get_is_training_placeholder(),
                    }, obs_space, act_space, 1, self.config["model"])
                    for obs, obs_space, act_space in zip(
                        obs_n, obs_space_n, act_space_n)
                ]
                out_n = [model.last_layer for model in model_n]
                out = tf.concat(out_n + act_n, axis=1)
            else:
                model_n = [None] * len(obs_n)
                out = tf.concat(obs_n + act_n, axis=1)

            for hidden in hiddens:
                out = tf.layers.dense(out, units=hidden, activation=activation)
            feature = out
            out = tf.layers.dense(feature, units=1, activation=None)

        return out, feature, model_n, tf.global_variables(scope.name)

    def _build_actor_network(self,
                             obs,
                             obs_space,
                             act_space,
                             hiddens,
                             activation=None,
                             scope=None):
        from tensorflow.contrib.distributions import RelaxedOneHotCategorical
        with tf.variable_scope(scope, reuse=tf.AUTO_REUSE) as scope:
            if self.config["use_state_preprocessor"]:
                model = ModelCatalog.get_model({
                    "obs": obs,
                    "is_training": self._get_is_training_placeholder(),
                }, obs_space, act_space, 1, self.config["model"])
                out = model.last_layer
            else:
                model = None
                out = obs

            for hidden in hiddens:
                out = tf.layers.dense(out, units=hidden, activation=activation)
            feature = tf.layers.dense(
                out, units=act_space.shape[0], activation=None)
            sampler = RelaxedOneHotCategorical(
                temperature=1.0, logits=feature).sample()

        return sampler, feature, model, tf.global_variables(scope.name)

    def update_target(self, tau=None):
        if tau is not None:
            self.sess.run(self.update_target_vars, {self.tau: tau})
        else:
            self.sess.run(self.update_target_vars)
