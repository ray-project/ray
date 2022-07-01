import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import minimize_and_clip
from ray.rllib.evaluation.postprocessing import adjust_nstep
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY

import logging
from gym.spaces import Box, Discrete
import numpy as np

logger = logging.getLogger(__name__)

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()


class MADDPGPostprocessing:
    """Implements agentwise termination signal and n-step learning."""

    @override(Policy)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        # FIXME: Get done from info is required since agentwise done is not
        #  supported now.
        sample_batch[SampleBatch.DONES] = self.get_done_from_info(
            sample_batch[SampleBatch.INFOS]
        )

        # N-step Q adjustments
        if self.config["n_step"] > 1:
            adjust_nstep(self.config["n_step"], self.config["gamma"], sample_batch)

        return sample_batch


class MADDPGTFPolicy(MADDPGPostprocessing, TFPolicy):
    def __init__(self, obs_space, act_space, config):
        # _____ Initial Configuration
        config = dict(ray.rllib.algorithms.maddpg.maddpg.DEFAULT_CONFIG, **config)
        self.config = config
        self.global_step = tf1.train.get_or_create_global_step()

        # FIXME: Get done from info is required since agentwise done is not
        # supported now.
        self.get_done_from_info = np.vectorize(lambda info: info.get("done", False))

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
                return Box(low=np.zeros((space.n,)), high=np.ones((space.n,)))
            else:
                raise UnsupportedSpaceException(
                    "Space {} is not supported.".format(space)
                )

        obs_space_n = [
            _make_continuous_space(space or obs_space)
            for _, (_, space, _, _) in config["multiagent"]["policies"].items()
        ]
        act_space_n = [
            _make_continuous_space(space or act_space)
            for _, (_, _, space, _) in config["multiagent"]["policies"].items()
        ]

        # _____ Placeholders
        # Placeholders for policy evaluation and updates
        def _make_ph_n(space_n, name=""):
            return [
                tf1.placeholder(
                    tf.float32, shape=(None,) + space.shape, name=name + "_%d" % i
                )
                for i, space in enumerate(space_n)
            ]

        obs_ph_n = _make_ph_n(obs_space_n, SampleBatch.OBS)
        act_ph_n = _make_ph_n(act_space_n, SampleBatch.ACTIONS)
        new_obs_ph_n = _make_ph_n(obs_space_n, SampleBatch.NEXT_OBS)
        new_act_ph_n = _make_ph_n(act_space_n, "new_actions")
        rew_ph = tf1.placeholder(
            tf.float32, shape=None, name="rewards_{}".format(agent_id)
        )
        done_ph = tf1.placeholder(
            tf.float32, shape=None, name="dones_{}".format(agent_id)
        )

        if config["use_local_critic"]:
            obs_space_n, act_space_n = [obs_space_n[agent_id]], [act_space_n[agent_id]]
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
            config["use_state_preprocessor"],
            config["critic_hiddens"],
            getattr(tf.nn, config["critic_hidden_activation"]),
            scope="critic",
        )

        # Build critic network for t + 1.
        target_critic, _, _, target_critic_vars = self._build_critic_network(
            new_obs_ph_n,
            new_act_ph_n,
            obs_space_n,
            act_space_n,
            config["use_state_preprocessor"],
            config["critic_hiddens"],
            getattr(tf.nn, config["critic_hidden_activation"]),
            scope="target_critic",
        )

        # Build critic loss.
        td_error = tf.subtract(
            tf.stop_gradient(
                rew_ph
                + (1.0 - done_ph)
                * (config["gamma"] ** config["n_step"])
                * target_critic[:, 0]
            ),
            critic[:, 0],
        )
        critic_loss = tf.reduce_mean(td_error ** 2)

        # _____ Policy Network
        # Build actor network for t.
        act_sampler, actor_feature, actor_model, actor_vars = self._build_actor_network(
            obs_ph_n[agent_id],
            obs_space_n[agent_id],
            act_space_n[agent_id],
            config["use_state_preprocessor"],
            config["actor_hiddens"],
            getattr(tf.nn, config["actor_hidden_activation"]),
            scope="actor",
        )

        # Build actor network for t + 1.
        self.new_obs_ph = new_obs_ph_n[agent_id]
        self.target_act_sampler, _, _, target_actor_vars = self._build_actor_network(
            self.new_obs_ph,
            obs_space_n[agent_id],
            act_space_n[agent_id],
            config["use_state_preprocessor"],
            config["actor_hiddens"],
            getattr(tf.nn, config["actor_hidden_activation"]),
            scope="target_actor",
        )

        # Build actor loss.
        act_n = act_ph_n.copy()
        act_n[agent_id] = act_sampler
        critic, _, _, _ = self._build_critic_network(
            obs_ph_n,
            act_n,
            obs_space_n,
            act_space_n,
            config["use_state_preprocessor"],
            config["critic_hiddens"],
            getattr(tf.nn, config["critic_hidden_activation"]),
            scope="critic",
        )
        actor_loss = -tf.reduce_mean(critic)
        if config["actor_feature_reg"] is not None:
            actor_loss += config["actor_feature_reg"] * tf.reduce_mean(
                actor_feature ** 2
            )

        # _____ Losses
        self.losses = {"critic": critic_loss, "actor": actor_loss}

        # _____ Optimizers
        self.optimizers = {
            "critic": tf1.train.AdamOptimizer(config["critic_lr"]),
            "actor": tf1.train.AdamOptimizer(config["actor_lr"]),
        }

        # _____ Build variable update ops.
        self.tau = tf1.placeholder_with_default(config["tau"], shape=(), name="tau")

        def _make_target_update_op(vs, target_vs, tau):
            return [
                target_v.assign(tau * v + (1.0 - tau) * target_v)
                for v, target_v in zip(vs, target_vs)
            ]

        self.update_target_vars = _make_target_update_op(
            critic_vars + actor_vars, target_critic_vars + target_actor_vars, self.tau
        )

        def _make_set_weight_op(variables):
            vs = list()
            for v in variables.values():
                vs += v
            phs = [
                tf1.placeholder(
                    tf.float32, shape=v.get_shape(), name=v.name.split(":")[0] + "_ph"
                )
                for v in vs
            ]
            return tf.group(*[v.assign(ph) for v, ph in zip(vs, phs)]), phs

        self.vars = {
            "critic": critic_vars,
            "actor": actor_vars,
            "target_critic": target_critic_vars,
            "target_actor": target_actor_vars,
        }
        self.update_vars, self.vars_ph = _make_set_weight_op(self.vars)

        # _____ TensorFlow Initialization

        sess = tf1.get_default_session()
        assert sess

        def _make_loss_inputs(placeholders):
            return [(ph.name.split("/")[-1].split(":")[0], ph) for ph in placeholders]

        loss_inputs = _make_loss_inputs(
            obs_ph_n + act_ph_n + new_obs_ph_n + new_act_ph_n + [rew_ph, done_ph]
        )

        TFPolicy.__init__(
            self,
            obs_space,
            act_space,
            config=config,
            sess=sess,
            obs_input=obs_ph_n[agent_id],
            sampled_action=act_sampler,
            loss=actor_loss + critic_loss,
            loss_inputs=loss_inputs,
            dist_inputs=actor_feature,
        )

        del self.view_requirements["prev_actions"]
        del self.view_requirements["prev_rewards"]

        self.get_session().run(tf1.global_variables_initializer())

        # Hard initial update
        self.update_target(1.0)

    @override(TFPolicy)
    def optimizer(self):
        return None

    @override(TFPolicy)
    def gradients(self, optimizer, loss):
        self.gvs = {
            k: minimize_and_clip(
                optimizer,
                self.losses[k],
                self.vars[k],
                self.config["grad_norm_clipping"],
            )
            for k, optimizer in self.optimizers.items()
        }
        return self.gvs["critic"] + self.gvs["actor"]

    @override(TFPolicy)
    def build_apply_op(self, optimizer, grads_and_vars):
        critic_apply_op = self.optimizers["critic"].apply_gradients(self.gvs["critic"])

        with tf1.control_dependencies([tf1.assign_add(self.global_step, 1)]):
            with tf1.control_dependencies([critic_apply_op]):
                actor_apply_op = self.optimizers["actor"].apply_gradients(
                    self.gvs["actor"]
                )

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
        return {"_state": self.get_session().run(var_list)}

    @override(TFPolicy)
    def set_weights(self, weights):
        self.get_session().run(
            self.update_vars, feed_dict=dict(zip(self.vars_ph, weights["_state"]))
        )

    @override(Policy)
    def get_state(self):
        return TFPolicy.get_state(self)

    @override(Policy)
    def set_state(self, state):
        TFPolicy.set_state(self, state)

    def _build_critic_network(
        self,
        obs_n,
        act_n,
        obs_space_n,
        act_space_n,
        use_state_preprocessor,
        hiddens,
        activation=None,
        scope=None,
    ):
        with tf1.variable_scope(scope, reuse=tf1.AUTO_REUSE) as scope:
            if use_state_preprocessor:
                model_n = [
                    ModelCatalog.get_model(
                        SampleBatch(
                            obs=obs, _is_training=self._get_is_training_placeholder()
                        ),
                        obs_space,
                        act_space,
                        1,
                        self.config["model"],
                    )
                    for obs, obs_space, act_space in zip(
                        obs_n, obs_space_n, act_space_n
                    )
                ]
                out_n = [model.last_layer for model in model_n]
                out = tf.concat(out_n + act_n, axis=1)
            else:
                model_n = [None] * len(obs_n)
                out = tf.concat(obs_n + act_n, axis=1)

            for hidden in hiddens:
                out = tf1.layers.dense(out, units=hidden, activation=activation)
            feature = out
            out = tf1.layers.dense(feature, units=1, activation=None)

        return out, feature, model_n, tf1.global_variables(scope.name)

    def _build_actor_network(
        self,
        obs,
        obs_space,
        act_space,
        use_state_preprocessor,
        hiddens,
        activation=None,
        scope=None,
    ):
        with tf1.variable_scope(scope, reuse=tf1.AUTO_REUSE) as scope:
            if use_state_preprocessor:
                model = ModelCatalog.get_model(
                    SampleBatch(
                        obs=obs, _is_training=self._get_is_training_placeholder()
                    ),
                    obs_space,
                    act_space,
                    1,
                    self.config["model"],
                )
                out = model.last_layer
            else:
                model = None
                out = obs

            for hidden in hiddens:
                out = tf1.layers.dense(out, units=hidden, activation=activation)
            feature = tf1.layers.dense(out, units=act_space.shape[0], activation=None)
            sampler = tfp.distributions.RelaxedOneHotCategorical(
                temperature=1.0, logits=feature
            ).sample()

        return sampler, feature, model, tf1.global_variables(scope.name)

    def update_target(self, tau=None):
        if tau is not None:
            self.get_session().run(self.update_target_vars, {self.tau: tau})
        else:
            self.get_session().run(self.update_target_vars)
