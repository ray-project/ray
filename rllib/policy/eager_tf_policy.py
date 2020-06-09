"""Eager mode TF policy built using build_tf_policy().

It supports both traced and non-traced eager execution modes."""

import functools
import logging
import numpy as np
from gym.spaces import Tuple, Dict

from ray.util.debug import log_once
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray

tf = try_import_tf()
logger = logging.getLogger(__name__)


def _convert_to_tf(x):
    if isinstance(x, SampleBatch):
        x = {k: v for k, v in x.items() if k != SampleBatch.INFOS}
        return tf.nest.map_structure(_convert_to_tf, x)
    if isinstance(x, Policy):
        return x

    if x is not None:
        x = tf.nest.map_structure(
            lambda f: tf.convert_to_tensor(f) if f is not None else None, x)
    return x


def _convert_to_numpy(x):
    def _map(x):
        if isinstance(x, tf.Tensor):
            return x.numpy()
        return x

    try:
        return tf.nest.map_structure(_map, x)
    except AttributeError:
        raise TypeError(
            ("Object of type {} has no method to convert to numpy.").format(
                type(x)))


def convert_eager_inputs(func):
    @functools.wraps(func)
    def _func(*args, **kwargs):
        if tf.executing_eagerly():
            args = [_convert_to_tf(x) for x in args]
            # TODO(gehring): find a way to remove specific hacks
            kwargs = {
                k: _convert_to_tf(v)
                for k, v in kwargs.items()
                if k not in {"info_batch", "episodes"}
            }
        return func(*args, **kwargs)

    return _func


def convert_eager_outputs(func):
    @functools.wraps(func)
    def _func(*args, **kwargs):
        out = func(*args, **kwargs)
        if tf.executing_eagerly():
            out = tf.nest.map_structure(_convert_to_numpy, out)
        return out

    return _func


def _disallow_var_creation(next_creator, **kw):
    v = next_creator(**kw)
    raise ValueError("Detected a variable being created during an eager "
                     "forward pass. Variables should only be created during "
                     "model initialization: {}".format(v.name))


def traced_eager_policy(eager_policy_cls):
    """Wrapper that enables tracing for all eager policy methods.

    This is enabled by the --trace / "eager_tracing" config."""

    class TracedEagerPolicy(eager_policy_cls):
        def __init__(self, *args, **kwargs):
            self._traced_learn_on_batch = None
            self._traced_compute_actions = None
            self._traced_compute_gradients = None
            self._traced_apply_gradients = None
            super(TracedEagerPolicy, self).__init__(*args, **kwargs)

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def learn_on_batch(self, samples):

            if self._traced_learn_on_batch is None:
                self._traced_learn_on_batch = tf.function(
                    super(TracedEagerPolicy, self).learn_on_batch,
                    autograph=False)

            return self._traced_learn_on_batch(samples)

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def compute_actions(self,
                            obs_batch,
                            state_batches=None,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            explore=None,
                            timestep=None,
                            **kwargs):

            obs_batch = tf.convert_to_tensor(obs_batch)
            state_batches = _convert_to_tf(state_batches)
            prev_action_batch = _convert_to_tf(prev_action_batch)
            prev_reward_batch = _convert_to_tf(prev_reward_batch)

            if self._traced_compute_actions is None:
                self._traced_compute_actions = tf.function(
                    super(TracedEagerPolicy, self).compute_actions,
                    autograph=False)

            return self._traced_compute_actions(
                obs_batch, state_batches, prev_action_batch, prev_reward_batch,
                info_batch, episodes, explore, timestep, **kwargs)

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def compute_gradients(self, samples):

            if self._traced_compute_gradients is None:
                self._traced_compute_gradients = tf.function(
                    super(TracedEagerPolicy, self).compute_gradients,
                    autograph=False)

            return self._traced_compute_gradients(samples)

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def apply_gradients(self, grads):

            if self._traced_apply_gradients is None:
                self._traced_apply_gradients = tf.function(
                    super(TracedEagerPolicy, self).apply_gradients,
                    autograph=False)

            return self._traced_apply_gradients(grads)

    TracedEagerPolicy.__name__ = eager_policy_cls.__name__
    TracedEagerPolicy.__qualname__ = eager_policy_cls.__qualname__
    return TracedEagerPolicy


def build_eager_tf_policy(name,
                          loss_fn,
                          get_default_config=None,
                          postprocess_fn=None,
                          stats_fn=None,
                          optimizer_fn=None,
                          gradients_fn=None,
                          apply_gradients_fn=None,
                          grad_stats_fn=None,
                          extra_learn_fetches_fn=None,
                          extra_action_fetches_fn=None,
                          before_init=None,
                          before_loss_init=None,
                          after_init=None,
                          make_model=None,
                          action_sampler_fn=None,
                          action_distribution_fn=None,
                          mixins=None,
                          obs_include_prev_action_reward=True,
                          get_batch_divisibility_req=None):
    """Build an eager TF policy.

    An eager policy runs all operations in eager mode, which makes debugging
    much simpler, but has lower performance.

    You shouldn't need to call this directly. Rather, prefer to build a TF
    graph policy and use set {"framework": "tfe"} in the trainer config to have
    it automatically be converted to an eager policy.

    This has the same signature as build_tf_policy()."""

    base = add_mixins(Policy, mixins)

    class eager_policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            assert tf.executing_eagerly()
            self.framework = "tf"
            Policy.__init__(self, observation_space, action_space, config)
            self._is_training = False
            self._loss_initialized = False
            self._sess = None

            if get_default_config:
                config = dict(get_default_config(), **config)

            if before_init:
                before_init(self, observation_space, action_space, config)

            self.config = config
            self.dist_class = None
            if action_sampler_fn or action_distribution_fn:
                if not make_model:
                    raise ValueError(
                        "`make_model` is required if `action_sampler_fn` OR "
                        "`action_distribution_fn` is given")
            else:
                self.dist_class, logit_dim = ModelCatalog.get_action_dist(
                    action_space, self.config["model"])

            if make_model:
                self.model = make_model(self, observation_space, action_space,
                                        config)
            else:
                self.model = ModelCatalog.get_model_v2(
                    observation_space,
                    action_space,
                    logit_dim,
                    config["model"],
                    framework="tf",
                )
            self.exploration = self._create_exploration()
            self._state_in = [
                tf.convert_to_tensor(np.array([s]))
                for s in self.model.get_initial_state()
            ]
            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    np.array([observation_space.sample()])),
                SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                    [flatten_to_single_ndarray(action_space.sample())]),
                SampleBatch.PREV_REWARDS: tf.convert_to_tensor([0.]),
            }

            if action_distribution_fn:
                dist_inputs, self.dist_class, _ = action_distribution_fn(
                    self, self.model, input_dict[SampleBatch.CUR_OBS])
            else:
                self.model(input_dict, self._state_in,
                           tf.convert_to_tensor([1]))

            if before_loss_init:
                before_loss_init(self, observation_space, action_space, config)

            self._initialize_loss_with_dummy_batch()
            self._loss_initialized = True

            if optimizer_fn:
                self._optimizer = optimizer_fn(self, config)
            else:
                self._optimizer = tf.train.AdamOptimizer(config["lr"])

            if after_init:
                after_init(self, observation_space, action_space, config)

        @override(Policy)
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
            assert tf.executing_eagerly()
            # Call super's postprocess_trajectory first.
            sample_batch = Policy.postprocess_trajectory(self, sample_batch)
            if postprocess_fn:
                return postprocess_fn(self, sample_batch, other_agent_batches,
                                      episode)
            return sample_batch

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def learn_on_batch(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            self._apply_gradients(grads_and_vars)
            return stats

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def compute_gradients(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            grads = [g for g, v in grads_and_vars]
            return grads, stats

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def compute_actions(self,
                            obs_batch,
                            state_batches=None,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            explore=None,
                            timestep=None,
                            **kwargs):

            explore = explore if explore is not None else \
                self.config["explore"]
            timestep = timestep if timestep is not None else \
                self.global_timestep

            # TODO: remove python side effect to cull sources of bugs.
            self._is_training = False
            self._state_in = state_batches

            if tf.executing_eagerly():
                n = len(obs_batch)
            else:
                n = obs_batch.shape[0]
            seq_lens = tf.ones(n, dtype=tf.int32)

            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_batch),
                "is_training": tf.constant(False),
            }
            if obs_include_prev_action_reward:
                if prev_action_batch is not None:
                    input_dict[SampleBatch.PREV_ACTIONS] = \
                        tf.convert_to_tensor(prev_action_batch)
                if prev_reward_batch is not None:
                    input_dict[SampleBatch.PREV_REWARDS] = \
                        tf.convert_to_tensor(prev_reward_batch)

            # Use Exploration object.
            with tf.variable_creator_scope(_disallow_var_creation):
                if action_sampler_fn:
                    dist_inputs = None
                    state_out = []
                    actions, logp = self.action_sampler_fn(
                        self,
                        self.model,
                        input_dict[SampleBatch.CUR_OBS],
                        explore=explore,
                        timestep=timestep)
                else:
                    # Exploration hook before each forward pass.
                    self.exploration.before_compute_actions(
                        timestep=timestep, explore=explore)

                    if action_distribution_fn:
                        dist_inputs, dist_class, state_out = \
                            action_distribution_fn(
                                self, self.model,
                                input_dict[SampleBatch.CUR_OBS],
                                explore=explore,
                                timestep=timestep,
                                is_training=False)
                    else:
                        dist_class = self.dist_class
                        dist_inputs, state_out = self.model(
                            input_dict, state_batches, seq_lens)

                    action_dist = dist_class(dist_inputs, self.model)

                    # Get the exploration action from the forward results.
                    actions, logp = self.exploration.get_exploration_action(
                        action_distribution=action_dist,
                        timestep=timestep,
                        explore=explore)

            # Add default and custom fetches.
            extra_fetches = {}
            # Action-logp and action-prob.
            if logp is not None:
                extra_fetches[SampleBatch.ACTION_PROB] = tf.exp(logp)
                extra_fetches[SampleBatch.ACTION_LOGP] = logp
            # Action-dist inputs.
            if dist_inputs is not None:
                extra_fetches[SampleBatch.ACTION_DIST_INPUTS] = dist_inputs
            # Custom extra fetches.
            if extra_action_fetches_fn:
                extra_fetches.update(extra_action_fetches_fn(self))

            # Increase our global sampling timestep counter by 1.
            self.global_timestep += 1

            return actions, state_out, extra_fetches

        @override(Policy)
        def compute_log_likelihoods(self,
                                    actions,
                                    obs_batch,
                                    state_batches=None,
                                    prev_action_batch=None,
                                    prev_reward_batch=None):
            if action_sampler_fn and action_distribution_fn is None:
                raise ValueError("Cannot compute log-prob/likelihood w/o an "
                                 "`action_distribution_fn` and a provided "
                                 "`action_sampler_fn`!")

            seq_lens = tf.ones(len(obs_batch), dtype=tf.int32)
            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_batch),
                "is_training": tf.constant(False),
            }
            if obs_include_prev_action_reward:
                input_dict.update({
                    SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                        prev_action_batch),
                    SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                        prev_reward_batch),
                })

            # Exploration hook before each forward pass.
            self.exploration.before_compute_actions(explore=False)

            # Action dist class and inputs are generated via custom function.
            if action_distribution_fn:
                dist_inputs, dist_class, _ = action_distribution_fn(
                    self,
                    self.model,
                    input_dict[SampleBatch.CUR_OBS],
                    explore=False,
                    is_training=False)
                action_dist = dist_class(dist_inputs, self.model)
                log_likelihoods = action_dist.logp(actions)
            # Default log-likelihood calculation.
            else:
                dist_inputs, _ = self.model(input_dict, state_batches,
                                            seq_lens)
                dist_class = self.dist_class

            action_dist = dist_class(dist_inputs, self.model)
            log_likelihoods = action_dist.logp(actions)

            return log_likelihoods

        @override(Policy)
        def apply_gradients(self, gradients):
            self._apply_gradients(
                zip([(tf.convert_to_tensor(g) if g is not None else None)
                     for g in gradients], self.model.trainable_variables()))

        @override(Policy)
        def get_exploration_info(self):
            return _convert_to_numpy(self.exploration.get_info())

        @override(Policy)
        def get_weights(self):
            variables = self.variables()
            return [v.numpy() for v in variables]

        @override(Policy)
        def set_weights(self, weights):
            variables = self.variables()
            assert len(weights) == len(variables), (len(weights),
                                                    len(variables))
            for v, w in zip(variables, weights):
                v.assign(w)

        @override(Policy)
        def get_state(self):
            state = {"_state": super().get_state()}
            state["_optimizer_variables"] = self._optimizer.variables()
            return state

        @override(Policy)
        def set_state(self, state):
            state = state.copy()  # shallow copy
            # Set optimizer vars first.
            optimizer_vars = state.pop("_optimizer_variables", None)
            if optimizer_vars and self._optimizer.variables():
                logger.warning(
                    "Cannot restore an optimizer's state for tf eager! Keras "
                    "is not able to save the v1.x optimizers (from "
                    "tf.compat.v1.train) since they aren't compatible with "
                    "checkpoints.")
                for opt_var, value in zip(self._optimizer.variables(),
                                          optimizer_vars):
                    opt_var.assign(value)
            # Then the Policy's (NN) weights.
            super().set_state(state["_state"])

        def variables(self):
            """Return the list of all savable variables for this policy."""
            return self.model.variables()

        @override(Policy)
        def is_recurrent(self):
            return len(self._state_in) > 0

        @override(Policy)
        def num_state_tensors(self):
            return len(self._state_in)

        @override(Policy)
        def get_initial_state(self):
            return self.model.get_initial_state()

        def get_session(self):
            return None  # None implies eager

        def get_placeholder(self, ph):
            raise ValueError(
                "get_placeholder() is not allowed in eager mode. Try using "
                "rllib.utils.tf_ops.make_tf_callable() to write "
                "functions that work in both graph and eager mode.")

        def loss_initialized(self):
            return self._loss_initialized

        @override(Policy)
        def export_model(self, export_dir):
            pass

        @override(Policy)
        def export_checkpoint(self, export_dir):
            pass

        def _get_is_training_placeholder(self):
            return tf.convert_to_tensor(self._is_training)

        def _apply_gradients(self, grads_and_vars):
            if apply_gradients_fn:
                apply_gradients_fn(self, self._optimizer, grads_and_vars)
            else:
                self._optimizer.apply_gradients(grads_and_vars)

        def _compute_gradients(self, samples):
            """Computes and returns grads as eager tensors."""

            self._is_training = True

            with tf.GradientTape(persistent=gradients_fn is not None) as tape:
                # TODO: set seq len and state-in properly
                state_in = []
                for i in range(self.num_state_tensors()):
                    state_in.append(samples["state_in_{}".format(i)])
                self._state_in = state_in

                self._seq_lens = None
                if len(state_in) > 0:
                    self._seq_lens = tf.ones(
                        samples[SampleBatch.CUR_OBS].shape[0], dtype=tf.int32)
                    samples["seq_lens"] = self._seq_lens

                model_out, _ = self.model(samples, self._state_in,
                                          self._seq_lens)
                loss = loss_fn(self, self.model, self.dist_class, samples)

            variables = self.model.trainable_variables()

            if gradients_fn:

                class OptimizerWrapper:
                    def __init__(self, tape):
                        self.tape = tape

                    def compute_gradients(self, loss, var_list):
                        return list(
                            zip(self.tape.gradient(loss, var_list), var_list))

                grads_and_vars = gradients_fn(self, OptimizerWrapper(tape),
                                              loss)
            else:
                grads_and_vars = list(
                    zip(tape.gradient(loss, variables), variables))

            if log_once("grad_vars"):
                for _, v in grads_and_vars:
                    logger.info("Optimizing variable {}".format(v.name))

            grads = [g for g, v in grads_and_vars]
            stats = self._stats(self, samples, grads)
            return grads_and_vars, stats

        def _stats(self, outputs, samples, grads):

            fetches = {}
            if stats_fn:
                fetches[LEARNER_STATS_KEY] = {
                    k: v
                    for k, v in stats_fn(outputs, samples).items()
                }
            else:
                fetches[LEARNER_STATS_KEY] = {}

            if extra_learn_fetches_fn:
                fetches.update(
                    {k: v
                     for k, v in extra_learn_fetches_fn(self).items()})
            if grad_stats_fn:
                fetches.update({
                    k: v
                    for k, v in grad_stats_fn(self, samples, grads).items()
                })
            return fetches

        def _initialize_loss_with_dummy_batch(self):
            # Dummy forward pass to initialize any policy attributes, etc.
            dummy_batch = {
                SampleBatch.CUR_OBS: np.array(
                    [self.observation_space.sample()]),
                SampleBatch.NEXT_OBS: np.array(
                    [self.observation_space.sample()]),
                SampleBatch.DONES: np.array([False], dtype=np.bool),
                SampleBatch.REWARDS: np.array([0], dtype=np.float32),
            }
            if isinstance(self.action_space, Tuple) or isinstance(
                    self.action_space, Dict):
                dummy_batch[SampleBatch.ACTIONS] = [
                    flatten_to_single_ndarray(self.action_space.sample())
                ]
            else:
                dummy_batch[SampleBatch.ACTIONS] = tf.nest.map_structure(
                    lambda c: np.array([c]), self.action_space.sample())

            if obs_include_prev_action_reward:
                dummy_batch.update({
                    SampleBatch.PREV_ACTIONS: dummy_batch[SampleBatch.ACTIONS],
                    SampleBatch.PREV_REWARDS: dummy_batch[SampleBatch.REWARDS],
                })
            for i, h in enumerate(self._state_in):
                dummy_batch["state_in_{}".format(i)] = h
                dummy_batch["state_out_{}".format(i)] = h

            if self._state_in:
                dummy_batch["seq_lens"] = np.array([1], dtype=np.int32)

            # Convert everything to tensors.
            dummy_batch = tf.nest.map_structure(tf.convert_to_tensor,
                                                dummy_batch)

            # for IMPALA which expects a certain sample batch size.
            def tile_to(tensor, n):
                return tf.tile(tensor,
                               [n] + [1 for _ in tensor.shape.as_list()[1:]])

            if get_batch_divisibility_req:
                dummy_batch = tf.nest.map_structure(
                    lambda c: tile_to(c, get_batch_divisibility_req(self)),
                    dummy_batch)

            # Execute a forward pass to get self.action_dist etc initialized,
            # and also obtain the extra action fetches
            _, _, fetches = self.compute_actions(
                dummy_batch[SampleBatch.CUR_OBS],
                self._state_in,
                dummy_batch.get(SampleBatch.PREV_ACTIONS),
                dummy_batch.get(SampleBatch.PREV_REWARDS),
                explore=False)
            dummy_batch.update(fetches)

            postprocessed_batch = self.postprocess_trajectory(
                SampleBatch(dummy_batch))

            # model forward pass for the loss (needed after postprocess to
            # overwrite any tensor state from that call)
            self.model.from_batch(dummy_batch)

            postprocessed_batch = tf.nest.map_structure(
                lambda c: tf.convert_to_tensor(c), postprocessed_batch.data)

            loss_fn(self, self.model, self.dist_class, postprocessed_batch)
            if stats_fn:
                stats_fn(self, postprocessed_batch)

        @classmethod
        def with_tracing(cls):
            return traced_eager_policy(cls)

    eager_policy_cls.__name__ = name + "_eager"
    eager_policy_cls.__qualname__ = name + "_eager"
    return eager_policy_cls
