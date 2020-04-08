from gym.spaces import Box
import logging
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.ddpg.ddpg_model import DDPGModel
from ray.rllib.agents.ddpg.noop_model import NoopModel
from ray.rllib.agents.dqn.dqn_tf_policy import postprocess_nstep_and_prio, \
    PRIO_WEIGHTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Deterministic
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import huber_loss, minimize_and_clip, scope_vars, \
    make_tf_callable

tf = try_import_tf()

logger = logging.getLogger(__name__)

ACTION_SCOPE = "action"
POLICY_SCOPE = "policy"
POLICY_TARGET_SCOPE = "target_policy"
Q_SCOPE = "critic"
Q_TARGET_SCOPE = "target_critic"
TWIN_Q_SCOPE = "twin_critic"
TWIN_Q_TARGET_SCOPE = "twin_target_critic"

## Importance sampling weights for prioritized replay
#PRIO_WEIGHTS = "weights"


# Moved into policy factory.
#class DDPGPostprocessing:
#    """Implements n-step learning."""

#    @override(Policy)
#    def postprocess_trajectory(self,
#                               sample_batch,
#                               other_agent_batches=None,
#                               episode=None):
#        return postprocess_nstep_and_prio(self, sample_batch)


def build_ddpg_models(policy, observation_space, action_space, config):
    if config["model"]["custom_model"]:
        logger.warning(
            "Setting use_state_preprocessor=True since a custom model "
            "was specified.")
        config["use_state_preprocessor"] = True
    if not isinstance(action_space, Box):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for SAC.".format(action_space))
    if isinstance(action_space, Box) and len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space has multiple dimensions "
            "{}. ".format(action_space.shape) +
            "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API.")

    if policy.config["use_state_preprocessor"]:
        default_model = None  # catalog decides
    else:
        default_model = NoopModel

    #if config["use_state_preprocessor"]:
    #    default_model = None  # catalog decides
    #    num_outputs = 256  # arbitrary
    #    config["model"]["no_final_linear"] = True
    #else:
    #    default_model = NoopModel
    #    num_outputs = int(np.product(observation_space.shape))

    policy.model = ModelCatalog.get_model_v2(
        obs_space=observation_space,
        action_space=action_space,
        num_outputs=observation_space.shape[0],
        model_config=config["model"],
        framework="tf",
        model_interface=DDPGModel,
        default_model=default_model,
        name="ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"],
        add_layer_norm=(policy.config["exploration_config"].get("type") ==
                        "ParameterNoise"),
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=observation_space,
        action_space=action_space,
        num_outputs=observation_space.shape[0],
        model_config=config["model"],
        framework="tf",
        model_interface=DDPGModel,
        default_model=default_model,
        name="target_ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"],
        add_layer_norm=(policy.config["exploration_config"].get("type") ==
                        "ParameterNoise"),
    )

    return policy.model


    ## Build the policy-network.
    #with tf.variable_scope(POLICY_SCOPE) as scope:
    #    policy.policy_model = \
    #        _build_policy_network(policy, observation_space, action_space)
    #    policy.policy_vars = scope_vars(scope.name)

    ## Build the target policy network.
    #with tf.variable_scope(POLICY_TARGET_SCOPE) as scope:
    #    policy.target_policy_model = _build_policy_network(
    #        policy, observation_space, action_space)
    #    policy.target_policy_vars = scope_vars(scope.name)

    ## Q-net.
    #with tf.variable_scope(Q_SCOPE) as scope:
    #    # Q-values for given actions & observations in given current
    #    policy.q_model = _build_q_network(policy,
    #                                         #self.obs_t,
    #                                         observation_space,
    #                                         action_space)  #, self.act_t)
    #    policy.q_func_vars = scope_vars(scope.name)

    ## twin-Q-net.
    #if config["twin_q"]:
    #    with tf.variable_scope(TWIN_Q_SCOPE) as scope:
    #        policy.twin_q_model = _build_q_network(policy,
    #                                                       #self.obs_t,
    #                                                       observation_space,
    #                                                       action_space)  #self.act_t)
    #        policy.twin_q_func_vars = scope_vars(scope.name)

    ## target Q-net.
    #with tf.variable_scope(Q_TARGET_SCOPE) as scope:
    #    policy.target_q_model = _build_q_network(policy, # self.obs_tp1,
    #                                observation_space,
    #                                action_space)  #, policy_tp1_smoothed)
    #    policy.target_q_func_vars = scope_vars(scope.name)

    ## target twin-Q-net.
    #if config["twin_q"]:
    #    with tf.variable_scope(TWIN_Q_TARGET_SCOPE) as scope:
    #        policy.target_twin_q_model = _build_q_network(policy,
    #                                         #self.obs_tp1,
    #                                         observation_space,
    #                                         action_space)
    #                                         #policy_tp1_smoothed)
    #        policy.twin_target_q_func_vars = scope_vars(scope.name)

    # Return `policy_model` as the "main" model of policy (used for
    # `compute_action` forward passes).
    #return policy.policy_model


#def _build_policy_network(policy, obs_space, action_space):
#    if policy.config["use_state_preprocessor"]:
#        default_model = None  # catalog decides
#    else:
#        default_model = NoopModel

#    model = ModelCatalog.get_model_v2(
#        obs_space=obs_space,
#        action_space=action_space,
#        num_outputs=obs_space.shape[0],
#        model_config=policy.config["model"],
#        framework="tf",
#        default_model=default_model,
#        model_interface=DDPGActionModel,
#        name="policy_model",
#        # Extra agrs for DDPGActionModel.
#        actor_hiddens=policy.config["actor_hiddens"],
#        actor_hidden_activation=policy.config["actor_hidden_activation"],
#        add_layer_norm=(policy.config["exploration_config"].get("type") ==
#                        "ParameterNoise"),
#    )
#    return model


#def _build_q_network(policy, obs, obs_space, action_space, actions):
#    if policy.config["use_state_preprocessor"]:
#        q_model = ModelCatalog.get_model({
#            "obs": obs,
#            "is_training": policy._get_is_training_placeholder(),
#        }, obs_space, action_space, 1, policy.config["model"])
#        q_out = tf.concat([q_model.last_layer, actions], axis=1)
#    else:
#        q_model = None
#        q_out = tf.concat([obs, actions], axis=1)

#    activation = getattr(tf.nn, policy.config["critic_hidden_activation"])
#    for hidden in policy.config["critic_hiddens"]:
#        q_out = tf.layers.dense(q_out, units=hidden, activation=activation)
#    q_values = tf.layers.dense(q_out, units=1, activation=None)

#    return q_values, q_model


def get_distribution_inputs_and_class(policy,
                                      model,
                                      obs_batch,
                                      *,
                                      explore=True,
                                      **kwargs):
    model_out, _ = model({
        "obs": obs_batch,
        "is_training": policy._get_is_training_placeholder()
    }, [], None)
    dist_inputs = model.get_policy_output(model_out)

    return dist_inputs, Deterministic, []  # []=state out


def ddpg_actor_critic_loss(policy, model, _, train_batch):
    twin_q = policy.config["twin_q"]
    gamma = policy.config["gamma"]
    n_step = policy.config["n_step"]
    use_huber = policy.config["use_huber"]
    huber_threshold = policy.config["huber_threshold"]
    l2_reg = policy.config["l2_reg"]

    input_dict = {
    
    }

    model_out_t, _ = model({
        "obs": train_batch[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    model_out_tp1, _ = model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    target_model_out_tp1, _ = policy.target_model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    # policy network evaluation
    with tf.variable_scope(POLICY_SCOPE, reuse=True) as scope:
        prev_update_ops = set(
            tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        policy_t = model.get_policy_output(model_out_t)
        policy_batchnorm_update_ops = list(
            set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
            prev_update_ops)

    with tf.variable_scope(POLICY_TARGET_SCOPE):
        policy_tp1 = \
            policy.target_model.get_policy_output(target_model_out_tp1)
                #policy.target_model({
                #    "obs": train_batch[SampleBatch.NEXT_OBS],
                #    "is_training": policy._get_is_training_placeholder()
                #}))

    # Action outputs
    with tf.variable_scope(ACTION_SCOPE, reuse=True):
        if policy.config["smooth_target_policy"]:
            target_noise_clip = policy.config["target_noise_clip"]
            clipped_normal_sample = tf.clip_by_value(
                tf.random_normal(
                    tf.shape(policy_tp1),
                    stddev=policy.config["target_noise"]),
                -target_noise_clip, target_noise_clip)
            policy_tp1_smoothed = tf.clip_by_value(
                policy_tp1 + clipped_normal_sample,
                policy.action_space.low * tf.ones_like(policy_tp1),
                policy.action_space.high * tf.ones_like(policy_tp1))
        else:
            # no smoothing, just use deterministic actions
            policy_tp1_smoothed = policy_tp1

    # q network evaluation
    prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
    with tf.variable_scope(Q_SCOPE) as scope:
        # Q-values for given actions & observations in given current
        q_t = model.get_q_values(model_out_t
            #policy.model({
            #        "obs": train_batch[SampleBatch.CUR_OBS],
            #        "is_training": policy._get_is_training_placeholder()
            #})
            , train_batch[SampleBatch.ACTIONS])
                                             #, policy.q_model = _build_q_network(self,
                                             #self.obs_t,
                                             #observation_space,
                                             #action_space, self.act_t)
        #self.q_func_vars = scope_vars(scope.name)

    with tf.variable_scope(Q_SCOPE, reuse=True):
        # Q-values for current policy (no noise) in given current state
        q_t_det_policy = model.get_q_values(model_out_t
            #policy.model({
            #        "obs": train_batch[SampleBatch.CUR_OBS],
            #        "is_training": policy._get_is_training_placeholder()
            #})
            , policy_t)
                                             ##_ = _build_q_network(self,
                                             #self.obs_t,
                                             #observation_space,
                                             #action_space,
                                             #self.policy_t)

    if twin_q:
        with tf.variable_scope(TWIN_Q_SCOPE) as scope:
            twin_q_t = model.get_twin_q_values(model_out_t
                #policy.model({
                #    "obs": train_batch[SampleBatch.CUR_OBS],
                #    "is_training": policy._get_is_training_placeholder()
            #})
            , train_batch[SampleBatch.ACTIONS])
             # = _build_q_network(self,
             #                                              self.obs_t,
             #                                              observation_space,
             #                                              action_space,
             #                                              self.act_t)
             # self.twin_q_func_vars = scope_vars(scope.name)
    q_batchnorm_update_ops = list(
        set(tf.get_collection(
            tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

    # target q network evaluation
    with tf.variable_scope(Q_TARGET_SCOPE) as scope:
        q_tp1 = policy.target_model.get_q_values(target_model_out_tp1
            #policy.target_model({
            #    "obs": train_batch[SampleBatch.NEXT_OBS],
            #    "is_training": policy._get_is_training_placeholder()
            #}
            , policy_tp1_smoothed)
                #self, self.obs_tp1,
                #                    observation_space,
                #                    action_space, policy_tp1_smoothed)
        # target_q_func_vars = scope_vars(scope.name)

    if twin_q:
        with tf.variable_scope(TWIN_Q_TARGET_SCOPE) as scope:
            twin_q_tp1 = policy.target_model.get_twin_q_values(
                target_model_out_tp1
                #policy.target_model({
                #    "obs": train_batch[SampleBatch.NEXT_OBS],
                #    "is_training": policy._get_is_training_placeholder()
            #}
            , policy_tp1_smoothed)        #self,
                                            # self.obs_tp1,
                                            # observation_space,
                                            # action_space,
                                            # policy_tp1_smoothed)
            #twin_target_q_func_vars = scope_vars(scope.name)

    #if twin_q:
    #    self.critic_loss, self.actor_loss, self.td_error \
    #        = self._build_actor_critic_loss(
    #        q_t, q_tp1, q_t_det_policy, twin_q_t=twin_q_t,
    #        twin_q_tp1=twin_q_tp1)
    #else:
    #    self.critic_loss, self.actor_loss, self.td_error \
    #        = self._build_actor_critic_loss(
    #        q_t, q_tp1, q_t_det_policy)

    q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
    if twin_q:
        twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

    q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = \
        (1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)) * \
        q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = tf.stop_gradient(
        train_batch[SampleBatch.REWARDS] + gamma**n_step * q_tp1_best_masked)

    # compute the error (potentially clipped)
    if twin_q:
        td_error = q_t_selected - q_t_selected_target
        twin_td_error = twin_q_t_selected - q_t_selected_target
        td_error = td_error + twin_td_error
        if use_huber:
            errors = huber_loss(td_error, huber_threshold) \
                + huber_loss(twin_td_error, huber_threshold)
        else:
            errors = 0.5 * tf.square(td_error) + 0.5 * tf.square(
                twin_td_error)
    else:
        td_error = q_t_selected - q_t_selected_target
        if use_huber:
            errors = huber_loss(td_error, huber_threshold)
        else:
            errors = 0.5 * tf.square(td_error)

    critic_loss = tf.reduce_mean(train_batch[PRIO_WEIGHTS] * errors)
    actor_loss = -tf.reduce_mean(q_t_det_policy)

    # Add l2-regularization if required.
    if l2_reg is not None:
        for var in policy.model.policy_variables():
            if "bias" not in var.name:
                actor_loss += (l2_reg * tf.nn.l2_loss(var))
        for var in policy.model.q_variables():
            if "bias" not in var.name:
                critic_loss += (l2_reg * tf.nn.l2_loss(var))

    # Model self-supervised losses
    if policy.config["use_state_preprocessor"]:
        actor_loss = model.action_model.custom_loss(
            actor_loss, input_dict)
        critic_loss = model.q_net.custom_loss(
            critic_loss, input_dict)
        if model.twin_q_model:
            critic_loss = model.twin_q_net.custom_loss(
                critic_loss, input_dict)

    # Store values for stats function.
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.td_error = td_error
    policy.q_t = q_t

    # Return one loss value (even though we treat them separately in our
    # 2 optimizers: actor and critic).
    return policy.critic_loss + policy.actor_loss


def make_ddpg_optimizers(policy, config):
    # Create separate optimizers for actor & critic losses.
    policy._actor_optimizer = tf.train.AdamOptimizer(
        learning_rate=config["actor_lr"])
    policy._critic_optimizer = tf.train.AdamOptimizer(
        learning_rate=config["critic_lr"])
    return None


#class OBSOLETE_DDPGTFPolicy(TFPolicy):
    #def __init__(self, observation_space, action_space, config):
    #    self.observation_space = observation_space
    #    self.action_space = action_space
    #    config = dict(ray.rllib.agents.ddpg.ddpg.DEFAULT_CONFIG, **config)
    #    if not isinstance(action_space, Box):
    #        raise UnsupportedSpaceException(
    #            "Action space {} is not supported for DDPG.".format(
    #                action_space))
    #    if len(action_space.shape) > 1:
    #        raise UnsupportedSpaceException(
    #            "Action space has multiple dimensions "
    #            "{}. ".format(action_space.shape) +
    #            "Consider reshaping this into a single dimension, "
    #            "using a Tuple action space, or the multi-agent API.")

        # Should be provided by parent class (TFPolicy).
        #self.config = config
        # Create global step for counting the number of update operations.
        #self.global_step = tf.train.get_or_create_global_step()
        # Create sampling timestep placeholder.
        #timestep = tf.placeholder(tf.int32, (), name="timestep")

        # Moved into make_ddpg_optimizers function.
        # use separate optimizers for actor & critic
        #self._actor_optimizer = tf.train.AdamOptimizer(
        #    learning_rate=self.config["actor_lr"])
        #self._critic_optimizer = tf.train.AdamOptimizer(
        #    learning_rate=self.config["critic_lr"])

        # Observation inputs.
        #self.cur_observations = tf.placeholder(
        #    tf.float32,
        #    shape=(None, ) + observation_space.shape,
        #    name="cur_obs")

        # Moved to make_ddpg_models
        #with tf.variable_scope(POLICY_SCOPE) as scope:
        #    self.policy_model = \
        #        self._build_policy_network(observation_space, action_space)
        #    self.policy_vars = scope_vars(scope.name)
        
        # Moved into get_distribution_inputs_and_class.
        #    # Action distribution inputs.
        #    self._distribution_inputs = \
        #        self.policy_model.get_policy_out(self.policy_model({
        #            "obs": self.cur_observations,
        #            "is_training": self._get_is_training_placeholder()
        #        }))
        #self.model = self.policy_model

        #TODO: Make sure exporation as well as distribution are created properly.
        # Create exploration component.
        #self.exploration = self._create_exploration()
        #explore = tf.placeholder_with_default(True, (), name="is_exploring")
        # Action outputs.
        #with tf.variable_scope(ACTION_SCOPE):
        #    self.output_actions, _ = self.exploration.get_exploration_action(
        #        action_distribution=Deterministic(self._distribution_inputs,
        #                                          self.model),
        #        timestep=timestep,
        #        explore=explore)

        # Replay inputs.
        #self.obs_t = tf.placeholder(
        #    tf.float32,
        #    shape=(None, ) + observation_space.shape,
        #    name="observation")
        #self.act_t = tf.placeholder(
        #    tf.float32, shape=(None, ) + action_space.shape, name="action")
        #self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        #self.obs_tp1 = tf.placeholder(
        #    tf.float32, shape=(None, ) + observation_space.shape)
        #self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        #self.importance_weights = tf.placeholder(
        #    tf.float32, [None], name="weight")

        # Moved into loss.
        # policy network evaluation
        #with tf.variable_scope(POLICY_SCOPE, reuse=True) as scope:
        #    prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        #    self.policy_t = \
        #        self.policy_model.get_policy_out(self.policy_model({
        #            "obs": self.obs_t,
        #            "is_training": self._get_is_training_placeholder()
        #        }))
        #    policy_batchnorm_update_ops = list(
        #        set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
        #        prev_update_ops)

        # Moved into make_ddpg_models function.
        # target policy network evaluation
        #with tf.variable_scope(POLICY_TARGET_SCOPE) as scope:
        #    self.target_policy_model = self._build_policy_network(
        #        observation_space, action_space)
        #    target_policy_vars = scope_vars(scope.name)
        # Moved into loss.
        #    policy_tp1 = \
        #        self.target_policy_model.get_policy_out(
        #            self.target_policy_model({
        #                "obs": self.obs_tp1,
        #                "is_training": self._get_is_training_placeholder()
        #            }))

        # Moved into loss
        # Action outputs
        #with tf.variable_scope(ACTION_SCOPE, reuse=True):
        #    if config["smooth_target_policy"]:
        #        target_noise_clip = self.config["target_noise_clip"]
        #        clipped_normal_sample = tf.clip_by_value(
        #            tf.random_normal(
        #                tf.shape(policy_tp1),
        #                stddev=self.config["target_noise"]),
        #            -target_noise_clip, target_noise_clip)
        #        policy_tp1_smoothed = tf.clip_by_value(
        #            policy_tp1 + clipped_normal_sample,
        #            action_space.low * tf.ones_like(policy_tp1),
        #            action_space.high * tf.ones_like(policy_tp1))
        #    else:
        #        # no smoothing, just use deterministic actions
        #        policy_tp1_smoothed = policy_tp1

        # Moved into loss.
        # q network evaluation
        #prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        #with tf.variable_scope(Q_SCOPE) as scope:
        #    # Q-values for given actions & observations in given current
        #    q_t, self.q_model = _build_q_network(self,
        #        self.obs_t, observation_space, action_space, self.act_t)
        #    self.q_func_vars = scope_vars(scope.name)
        ## Moved into stats function.
        ##self.stats = {
        ##    "mean_q": tf.reduce_mean(q_t),
        ##    "max_q": tf.reduce_max(q_t),
        ##    "min_q": tf.reduce_min(q_t),
        ##}
        #with tf.variable_scope(Q_SCOPE, reuse=True):
        #    # Q-values for current policy (no noise) in given current state
        #    q_t_det_policy, _ = _build_q_network(self,
        #        self.obs_t, observation_space, action_space, self.policy_t)
        #if self.config["twin_q"]:
        #    with tf.variable_scope(TWIN_Q_SCOPE) as scope:
        #        twin_q_t, self.twin_q_model = _build_q_network(self,
        #            self.obs_t, observation_space, action_space, self.act_t)
        #        self.twin_q_func_vars = scope_vars(scope.name)
        #q_batchnorm_update_ops = list(
        #    set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)
        #
        ## target q network evaluation
        #with tf.variable_scope(Q_TARGET_SCOPE) as scope:
        #    q_tp1, _ = _build_q_network(self, self.obs_tp1, observation_space,
        #                                     action_space, policy_tp1_smoothed)
        #    target_q_func_vars = scope_vars(scope.name)
        #if self.config["twin_q"]:
        #    with tf.variable_scope(TWIN_Q_TARGET_SCOPE) as scope:
        #        twin_q_tp1, _ = _build_q_network(self,
        #            self.obs_tp1, observation_space, action_space,
        #            policy_tp1_smoothed)
        #        twin_target_q_func_vars = scope_vars(scope.name)
        #
        #if self.config["twin_q"]:
        #    self.critic_loss, self.actor_loss, self.td_error \
        #        = self._build_actor_critic_loss(
        #            q_t, q_tp1, q_t_det_policy, twin_q_t=twin_q_t,
        #            twin_q_tp1=twin_q_tp1)
        #else:
        #    self.critic_loss, self.actor_loss, self.td_error \
        #        = self._build_actor_critic_loss(
        #            q_t, q_tp1, q_t_det_policy)
        #
        #if config["l2_reg"] is not None:
        #    for var in self.policy_vars:
        #        if "bias" not in var.name:
        #            self.actor_loss += (config["l2_reg"] * tf.nn.l2_loss(var))
        #    for var in self.q_func_vars:
        #        if "bias" not in var.name:
        #            self.critic_loss += (config["l2_reg"] * tf.nn.l2_loss(var))
        #    if self.config["twin_q"]:
        #        for var in self.twin_q_func_vars:
        #            if "bias" not in var.name:
        #                self.critic_loss += (
        #                    config["l2_reg"] * tf.nn.l2_loss(var))

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        #self.tau_value = config.get("tau")
        #self.tau = tf.placeholder(tf.float32, (), name="tau")
        #update_target_expr = []
        #for var, var_target in zip(
        #        sorted(self.q_func_vars, key=lambda v: v.name),
        #        sorted(target_q_func_vars, key=lambda v: v.name)):
        #    update_target_expr.append(
        #        var_target.assign(self.tau * var +
        #                          (1.0 - self.tau) * var_target))
        #if self.config["twin_q"]:
        #    for var, var_target in zip(
        #            sorted(self.twin_q_func_vars, key=lambda v: v.name),
        #            sorted(twin_target_q_func_vars, key=lambda v: v.name)):
        #        update_target_expr.append(
        #            var_target.assign(self.tau * var +
        #                              (1.0 - self.tau) * var_target))
        #for var, var_target in zip(
        #        sorted(self.policy_vars, key=lambda v: v.name),
        #        sorted(self.target_policy_vars, key=lambda v: v.name)):
        #    update_target_expr.append(
        #        var_target.assign(self.tau * var +
        #                          (1.0 - self.tau) * var_target))
        #self.update_target_expr = tf.group(*update_target_expr)

        #self.sess = tf.get_default_session()
        #self.loss_inputs = [
        #    (SampleBatch.CUR_OBS, self.obs_t),
        #    (SampleBatch.ACTIONS, self.act_t),
        #    (SampleBatch.REWARDS, self.rew_t),
        #    (SampleBatch.NEXT_OBS, self.obs_tp1),
        #    (SampleBatch.DONES, self.done_mask),
        #    (PRIO_WEIGHTS, self.importance_weights),
        #]
        #input_dict = dict(self.loss_inputs)

        # moved to loss function
        #if self.config["use_state_preprocessor"]:
        #    # Model self-supervised losses
        #    self.actor_loss = self.policy_model.custom_loss(
        #        self.actor_loss, input_dict)
        #    self.critic_loss = self.q_model.custom_loss(
        #        self.critic_loss, input_dict)
        #    if self.config["twin_q"]:
        #        self.critic_loss = self.twin_q_model.custom_loss(
        #            self.critic_loss, input_dict)

        #TFPolicy.__init__(
        #    self,
        #    observation_space,
        #    action_space,
        #    self.config,
        #    self.sess,
        #    #obs_input=self.cur_observations,
        #    sampled_action=self.output_actions,
        #    loss=self.actor_loss + self.critic_loss,
        #    loss_inputs=self.loss_inputs,
        #    update_ops=q_batchnorm_update_ops + policy_batchnorm_update_ops,
        #    explore=explore,
        #    dist_inputs=self._distribution_inputs,
        #    dist_class=Deterministic,
        #    timestep=timestep)
        ##self.sess.run(tf.global_variables_initializer())

        # Note that this encompasses both the policy and Q-value networks and
        # their corresponding target networks
        #self.variables = ray.experimental.tf_utils.TensorFlowVariables(
        #    tf.group(q_t_det_policy, q_tp1, self._actor_optimizer.variables(),
        #             self._critic_optimizer.variables()), self.sess)

        # Hard initial update
        #self.update_target(tau=1.0)

    # Moved to make_ddpg_optimizers.
    #@override(TFPolicy)
    #def optimizer(self):
    #    # we don't use this because we have two separate optimisers
    #    return None

def build_apply_op(policy, optimizer, grads_and_vars):
    # for policy gradient, update policy net one time v.s.
    # update critic net `policy_delay` time(s)
    should_apply_actor_opt = tf.equal(
        tf.mod(policy.global_step, policy.config["policy_delay"]), 0)

    def make_apply_op():
        return policy._actor_optimizer.apply_gradients(
            policy._actor_grads_and_vars)

    actor_op = tf.cond(
        should_apply_actor_opt,
        true_fn=make_apply_op,
        false_fn=lambda: tf.no_op())
    critic_op = policy._critic_optimizer.apply_gradients(
        policy._critic_grads_and_vars)
    # increment global step & apply ops
    with tf.control_dependencies([tf.assign_add(policy.global_step, 1)]):
        return tf.group(actor_op, critic_op)


def gradients_fn(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"] is not None:
        actor_grads_and_vars = minimize_and_clip(
            policy._actor_optimizer,
            policy.actor_loss,
            var_list=policy.model.policy_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        critic_grads_and_vars = minimize_and_clip(
            policy._critic_optimizer,
            policy.critic_loss,
            var_list=policy.model.q_variables(),
                #if policy.config["twin_q"] else self.q_func_vars,
            clip_val=policy.config["grad_norm_clipping"])
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables())
        #if policy.config["twin_q"]:
        #    critic_vars = self.q_func_vars + self.twin_q_func_vars
        #else:
        #    critic_vars = self.q_func_vars
        critic_grads_and_vars = policy._critic_optimizer.compute_gradients(
            policy.critic_loss, var_list=policy.model.q_variables())
    # save these for later use in build_apply_op
    policy._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                  if g is not None]
    policy._critic_grads_and_vars = [(g, v)
                                   for (g, v) in critic_grads_and_vars
                                   if g is not None]
    grads_and_vars = policy._actor_grads_and_vars + \
        policy._critic_grads_and_vars
    return grads_and_vars


#def extra_compute_grad_fetches(self):
#    return {
#        "td_error": self.td_error,
#        LEARNER_STATS_KEY: self.stats,
#    }

    #@override(TFPolicy)
    #def get_weights(self):
    #    return self.variables.get_weights()

    #@override(TFPolicy)
    #def set_weights(self, weights):
    #    self.variables.set_weights(weights)

    #def _build_q_network(self, obs, obs_space, action_space, actions):
    #    if self.config["use_state_preprocessor"]:
    #        q_model = ModelCatalog.get_model({
    #            "obs": obs,
    #            "is_training": self._get_is_training_placeholder(),
    #        }, obs_space, action_space, 1, self.config["model"])
    #        q_out = tf.concat([q_model.last_layer, actions], axis=1)
    #    else:
    #        q_model = None
    #        q_out = tf.concat([obs, actions], axis=1)

    #    activation = getattr(tf.nn, self.config["critic_hidden_activation"])
    #    for hidden in self.config["critic_hiddens"]:
    #        q_out = tf.layers.dense(q_out, units=hidden, activation=activation)
    #    q_values = tf.layers.dense(q_out, units=1, activation=None)

    #    return q_values, q_model

    #def _build_policy_network(self, obs_space, action_space):
    #    if self.config["use_state_preprocessor"]:
    #        default_model = None  # catalog decides
    #    else:
    #        default_model = NoopModel

    #    model = ModelCatalog.get_model_v2(
    #        obs_space=obs_space,
    #        action_space=action_space,
    #        num_outputs=obs_space.shape[0],
    #        model_config=self.config["model"],
    #        framework="tf",
    #        default_model=default_model,
    #        model_interface=DDPGActionModel,
    #        name="policy_model",
    #        # Extra agrs for DDPGActionModel.
    #        actor_hiddens=self.config["actor_hiddens"],
    #        actor_hidden_activation=self.config["actor_hidden_activation"],
    #        add_layer_norm=(self.config["exploration_config"].get("type") ==
    #                        "ParameterNoise"),
    #    )
    #    return model

    #def _build_actor_critic_loss(self,
    #                             q_t,
    #                             q_tp1,
    #                             q_t_det_policy,
    #                             twin_q_t=None,
    #                             twin_q_tp1=None):
    #    twin_q = self.config["twin_q"]
    #    gamma = self.config["gamma"]
    #    n_step = self.config["n_step"]
    #    use_huber = self.config["use_huber"]
    #    huber_threshold = self.config["huber_threshold"]

    #    q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
    #    if twin_q:
    #        twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
    #        q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

    #    q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    #    q_tp1_best_masked = (1.0 - self.done_mask) * q_tp1_best

    #    # compute RHS of bellman equation
    #    q_t_selected_target = tf.stop_gradient(
    #        self.rew_t + gamma**n_step * q_tp1_best_masked)

    #    # compute the error (potentially clipped)
    #    if twin_q:
    #        td_error = q_t_selected - q_t_selected_target
    #        twin_td_error = twin_q_t_selected - q_t_selected_target
    #        td_error = td_error + twin_td_error
    #        if use_huber:
    #            errors = huber_loss(td_error, huber_threshold) \
    #                + huber_loss(twin_td_error, huber_threshold)
    #        else:
    #            errors = 0.5 * tf.square(td_error) + 0.5 * tf.square(
    #                twin_td_error)
    #    else:
    #        td_error = q_t_selected - q_t_selected_target
    #        if use_huber:
    #            errors = huber_loss(td_error, huber_threshold)
    #        else:
    #            errors = 0.5 * tf.square(td_error)

    #    critic_loss = tf.reduce_mean(self.importance_weights * errors)
    #    actor_loss = -tf.reduce_mean(q_t_det_policy)
    #    return critic_loss, actor_loss, td_error

    #def compute_td_error(self, obs_t, act_t, rew_t, obs_tp1, done_mask,
    #                     importance_weights):
    #    td_err = self.sess.run(
    #        self.td_error,
    #        feed_dict={
    #            self.obs_t: [np.array(ob) for ob in obs_t],
    #            self.act_t: act_t,
    #            self.rew_t: rew_t,
    #            self.obs_tp1: [np.array(ob) for ob in obs_tp1],
    #            self.done_mask: done_mask,
    #            self.importance_weights: importance_weights
    #        })
    #    return td_err

    # Moved into Target
    # support both hard and soft sync
    #def update_target(self, tau=None):
    #    tau = tau or self.tau_value
    #    return self.sess.run(
    #        self.update_target_expr, feed_dict={self.tau: tau})


def build_ddpg_stats(policy, batch):
    stats = {
        "mean_q": tf.reduce_mean(policy.q_t),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }
    return stats


def before_init_fn(policy, obs_space, action_space, config):
    # Create global step for counting the number of update operations.
    policy.global_step = tf.train.get_or_create_global_step()


class ComputeTDErrorMixin:
    def __init__(self, loss_fn):
        @make_tf_callable(self.get_session(), dynamic_shape=True)
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask,
                             importance_weights):
            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
            loss_fn(
                self, self.model, None, {
                    SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_t),
                    SampleBatch.ACTIONS: tf.convert_to_tensor(act_t),
                    SampleBatch.REWARDS: tf.convert_to_tensor(rew_t),
                    SampleBatch.NEXT_OBS: tf.convert_to_tensor(obs_tp1),
                    SampleBatch.DONES: tf.convert_to_tensor(done_mask),
                    PRIO_WEIGHTS: tf.convert_to_tensor(importance_weights),
                })
            # `self.td_error` is set in loss_fn.
            return self.td_error

        self.compute_td_error = compute_td_error


def setup_mid_mixins(policy, obs_space, action_space, config):
    ComputeTDErrorMixin.__init__(policy, ddpg_actor_critic_loss)


class TargetNetworkMixin:
    def __init__(self, config):
        @make_tf_callable(self.get_session())
        def update_target_fn(tau):
            tau = tf.convert_to_tensor(tau, dtype=tf.float32)
            update_target_expr = []
            model_vars = self.model.trainable_variables()
            target_model_vars = self.target_model.trainable_variables()
            assert len(model_vars) == len(target_model_vars), \
                (model_vars, target_model_vars)
            for var, var_target in zip(model_vars, target_model_vars):
                update_target_expr.append(
                    var_target.assign(tau * var + (1.0 - tau) * var_target))
                logger.debug("Update target op {}".format(var_target))
            return tf.group(*update_target_expr)

        # Hard initial update
        self._do_update = update_target_fn
        self.update_target(tau=1.0)

    # support both hard and soft sync
    def update_target(self, tau=None):
        self._do_update(np.float32(tau or self.config.get("tau")))

    @override(TFPolicy)
    def variables(self):
        return self.model.variables() + self.target_model.variables()


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, config)


DDPGTFPolicy = build_tf_policy(
    name="DQNTFPolicy",
    get_default_config=lambda: ray.rllib.agents.ddpg.ddpg.DEFAULT_CONFIG,
    make_model=build_ddpg_models,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=ddpg_actor_critic_loss,
    stats_fn=build_ddpg_stats,
    postprocess_fn=postprocess_nstep_and_prio,
    optimizer_fn=make_ddpg_optimizers,

    gradients_fn=gradients_fn,
    apply_gradients_fn=build_apply_op,

    #extra_action_fetches_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    before_init=before_init_fn,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
    #    LearningRateSchedule,
    ]
)
