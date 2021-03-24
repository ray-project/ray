"""Eager mode TF policy built using build_tf_policy().

It supports both traced and non-traced eager execution modes."""

import functools
import logging
import threading
from typing import Dict, List, Optional, Tuple

from ray.util.debug import log_once
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


def _convert_to_tf(x, dtype=None):
    if isinstance(x, SampleBatch):
        x = {k: v for k, v in x.items() if k != SampleBatch.INFOS}
        return tf.nest.map_structure(_convert_to_tf, x)
    elif isinstance(x, Policy):
        return x
    # Special handling of "Repeated" values.
    elif isinstance(x, RepeatedValues):
        return RepeatedValues(
            tf.nest.map_structure(_convert_to_tf, x.values), x.lengths,
            x.max_len)

    if x is not None:
        d = dtype
        x = tf.nest.map_structure(
            lambda f: tf.convert_to_tensor(f, d) if f is not None else None, x)
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
            # TODO: (sven) find a way to remove key-specific hacks.
            kwargs = {
                k: _convert_to_tf(
                    v, dtype=tf.int64 if k == "timestep" else None)
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

        @override(eager_policy_cls)
        @convert_eager_inputs
        @convert_eager_outputs
        def _learn_on_batch_eager(self, samples):

            if self._traced_learn_on_batch is None:
                self._traced_learn_on_batch = tf.function(
                    super(TracedEagerPolicy, self)._learn_on_batch_eager,
                    autograph=False,
                    experimental_relax_shapes=True)

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
                    autograph=False,
                    experimental_relax_shapes=True)

            return self._traced_compute_actions(
                obs_batch, state_batches, prev_action_batch, prev_reward_batch,
                info_batch, episodes, explore, timestep, **kwargs)

        @override(eager_policy_cls)
        @convert_eager_inputs
        @convert_eager_outputs
        def _compute_gradients_eager(self, samples):

            if self._traced_compute_gradients is None:
                self._traced_compute_gradients = tf.function(
                    super(TracedEagerPolicy, self).compute_gradients,
                    autograph=False,
                    experimental_relax_shapes=True)

            return self._traced_compute_gradients(samples)

        @override(Policy)
        @convert_eager_inputs
        @convert_eager_outputs
        def apply_gradients(self, grads):

            if self._traced_apply_gradients is None:
                self._traced_apply_gradients = tf.function(
                    super(TracedEagerPolicy, self).apply_gradients,
                    autograph=False,
                    experimental_relax_shapes=True)

            return self._traced_apply_gradients(grads)

    TracedEagerPolicy.__name__ = eager_policy_cls.__name__
    TracedEagerPolicy.__qualname__ = eager_policy_cls.__qualname__
    return TracedEagerPolicy


def build_eager_tf_policy(
        name,
        loss_fn,
        get_default_config=None,
        postprocess_fn=None,
        stats_fn=None,
        optimizer_fn=None,
        gradients_fn=None,
        apply_gradients_fn=None,
        grad_stats_fn=None,
        extra_learn_fetches_fn=None,
        extra_action_out_fn=None,
        validate_spaces=None,
        before_init=None,
        before_loss_init=None,
        after_init=None,
        make_model=None,
        action_sampler_fn=None,
        action_distribution_fn=None,
        mixins=None,
        obs_include_prev_action_reward=DEPRECATED_VALUE,
        get_batch_divisibility_req=None,
        # Deprecated args.
        extra_action_fetches_fn=None):
    """Build an eager TF policy.

    An eager policy runs all operations in eager mode, which makes debugging
    much simpler, but has lower performance.

    You shouldn't need to call this directly. Rather, prefer to build a TF
    graph policy and use set {"framework": "tfe"} in the trainer config to have
    it automatically be converted to an eager policy.

    This has the same signature as build_tf_policy()."""

    base = add_mixins(Policy, mixins)

    if extra_action_fetches_fn is not None:
        deprecation_warning(
            old="extra_action_fetches_fn",
            new="extra_action_out_fn",
            error=False)
        extra_action_out_fn = extra_action_fetches_fn

    if obs_include_prev_action_reward != DEPRECATED_VALUE:
        deprecation_warning(old="obs_include_prev_action_reward", error=False)

    class eager_policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            assert tf.executing_eagerly()
            self.framework = config.get("framework", "tfe")
            Policy.__init__(self, observation_space, action_space, config)
            self._is_training = False
            self._loss_initialized = False
            self._sess = None

            self._loss = loss_fn
            self.batch_divisibility_req = get_batch_divisibility_req(self) if \
                callable(get_batch_divisibility_req) else \
                (get_batch_divisibility_req or 1)
            self._max_seq_len = config["model"]["max_seq_len"]

            if get_default_config:
                config = dict(get_default_config(), **config)

            if validate_spaces:
                validate_spaces(self, observation_space, action_space, config)

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
                    framework=self.framework,
                )
            # Lock used for locking some methods on the object-level.
            # This prevents possible race conditions when calling the model
            # first, then its value function (e.g. in a loss function), in
            # between of which another model call is made (e.g. to compute an
            # action).
            self._lock = threading.RLock()

            # Auto-update model's inference view requirements, if recurrent.
            self._update_model_view_requirements_from_init_state()

            self.exploration = self._create_exploration()
            self._state_inputs = self.model.get_initial_state()
            self._is_recurrent = len(self._state_inputs) > 0

            # Combine view_requirements for Model and Policy.
            self.view_requirements.update(self.model.view_requirements)

            if before_loss_init:
                before_loss_init(self, observation_space, action_space, config)

            if optimizer_fn:
                optimizers = optimizer_fn(self, config)
            else:
                optimizers = tf.keras.optimizers.Adam(config["lr"])
            optimizers = force_list(optimizers)
            if getattr(self, "exploration", None):
                optimizers = self.exploration.get_exploration_optimizer(
                    optimizers)
            # TODO: (sven) Allow tf policy to have more than 1 optimizer.
            #  Just like torch Policy does.
            self._optimizer = optimizers[0] if optimizers else None

            self._initialize_loss_from_dummy_batch(
                auto_remove_unneeded_view_reqs=True,
                stats_fn=stats_fn,
            )
            self._loss_initialized = True

            if after_init:
                after_init(self, observation_space, action_space, config)

            # Got to reset global_timestep again after fake run-throughs.
            self.global_timestep = 0

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

        @with_lock
        @override(Policy)
        def learn_on_batch(self, postprocessed_batch):
            # Callback handling.
            learn_stats = {}
            self.callbacks.on_learn_on_batch(
                policy=self,
                train_batch=postprocessed_batch,
                result=learn_stats)

            if not isinstance(postprocessed_batch, SampleBatch) or \
                    not postprocessed_batch.zero_padded:
                pad_batch_to_sequences_of_same_size(
                    postprocessed_batch,
                    max_seq_len=self._max_seq_len,
                    shuffle=False,
                    batch_divisibility_req=self.batch_divisibility_req,
                    view_requirements=self.view_requirements,
                )
            else:
                postprocessed_batch["seq_lens"] = postprocessed_batch.seq_lens

            self._is_training = True
            postprocessed_batch["is_training"] = True
            stats = self._learn_on_batch_eager(postprocessed_batch)
            stats.update({"custom_metrics": learn_stats})
            return stats

        @convert_eager_inputs
        @convert_eager_outputs
        def _learn_on_batch_eager(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            self._apply_gradients(grads_and_vars)
            return stats

        @override(Policy)
        def compute_gradients(self, samples):
            pad_batch_to_sequences_of_same_size(
                samples,
                shuffle=False,
                max_seq_len=self._max_seq_len,
                batch_divisibility_req=self.batch_divisibility_req)

            self._is_training = True
            samples["is_training"] = True
            return self._compute_gradients_eager(samples)

        @convert_eager_inputs
        @convert_eager_outputs
        def _compute_gradients_eager(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            grads = [g for g, v in grads_and_vars]
            return grads, stats

        @override(Policy)
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

            self._is_training = False
            self._is_recurrent = \
                state_batches is not None and state_batches != []

            if not tf1.executing_eagerly():
                tf1.enable_eager_execution()

            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_batch),
                "is_training": tf.constant(False),
            }
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = \
                    tf.convert_to_tensor(prev_action_batch)
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = \
                    tf.convert_to_tensor(prev_reward_batch)

            return self._compute_action_helper(input_dict, state_batches,
                                               episodes, explore, timestep)

        @override(Policy)
        def compute_actions_from_input_dict(
                self,
                input_dict: Dict[str, TensorType],
                explore: bool = None,
                timestep: Optional[int] = None,
                **kwargs
        ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

            if not tf1.executing_eagerly():
                tf1.enable_eager_execution()

            # Pass lazy (torch) tensor dict to Model as `input_dict`.
            input_dict = self._lazy_tensor_dict(input_dict)
            # Pack internal state inputs into (separate) list.
            state_batches = [
                input_dict[k] for k in input_dict.keys() if "state_in" in k[:8]
            ]

            return self._compute_action_helper(input_dict, state_batches, None,
                                               explore, timestep)

        @with_lock
        @convert_eager_inputs
        @convert_eager_outputs
        def _compute_action_helper(self, input_dict, state_batches, episodes,
                                   explore, timestep):

            explore = explore if explore is not None else \
                self.config["explore"]
            timestep = timestep if timestep is not None else \
                self.global_timestep
            if isinstance(timestep, tf.Tensor):
                timestep = int(timestep.numpy())
            self._is_training = False
            self._state_in = state_batches or []
            # Calculate RNN sequence lengths.
            batch_size = input_dict[SampleBatch.CUR_OBS].shape[0]
            seq_lens = tf.ones(batch_size, dtype=tf.int32) if state_batches \
                else None

            # Use Exploration object.
            with tf.variable_creator_scope(_disallow_var_creation):
                if action_sampler_fn:
                    dist_inputs = None
                    state_out = []
                    actions, logp = action_sampler_fn(
                        self,
                        self.model,
                        input_dict[SampleBatch.CUR_OBS],
                        explore=explore,
                        timestep=timestep,
                        episodes=episodes)
                else:
                    # Exploration hook before each forward pass.
                    self.exploration.before_compute_actions(
                        timestep=timestep, explore=explore)

                    if action_distribution_fn:

                        # Try new action_distribution_fn signature, supporting
                        # state_batches and seq_lens.
                        try:
                            dist_inputs, self.dist_class, state_out = \
                                action_distribution_fn(
                                    self,
                                    self.model,
                                    input_dict=input_dict,
                                    state_batches=state_batches,
                                    seq_lens=seq_lens,
                                    explore=explore,
                                    timestep=timestep,
                                    is_training=False)
                        # Trying the old way (to stay backward compatible).
                        # TODO: Remove in future.
                        except TypeError as e:
                            if "positional argument" in e.args[0] or \
                                    "unexpected keyword argument" in e.args[0]:
                                dist_inputs, self.dist_class, state_out = \
                                    action_distribution_fn(
                                        self, self.model,
                                        input_dict[SampleBatch.CUR_OBS],
                                        explore=explore,
                                        timestep=timestep,
                                        is_training=False)
                            else:
                                raise e
                    else:
                        dist_inputs, state_out = self.model(
                            input_dict, state_batches, seq_lens)

                    action_dist = self.dist_class(dist_inputs, self.model)

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
            if extra_action_out_fn:
                extra_fetches.update(extra_action_out_fn(self))

            # Update our global timestep by the batch size.
            self.global_timestep += int(batch_size)

            return actions, state_out, extra_fetches

        @with_lock
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
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = \
                    tf.convert_to_tensor(prev_action_batch)
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = \
                    tf.convert_to_tensor(prev_reward_batch)

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
        def get_weights(self, as_dict=False):
            variables = self.variables()
            if as_dict:
                return {v.name: v.numpy() for v in variables}
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
            return self._is_recurrent

        @override(Policy)
        def num_state_tensors(self):
            return len(self._state_inputs)

        @override(Policy)
        def get_initial_state(self):
            if hasattr(self, "model"):
                return self.model.get_initial_state()
            return []

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
                self._optimizer.apply_gradients(
                    [(g, v) for g, v in grads_and_vars if g is not None])

        @with_lock
        def _compute_gradients(self, samples):
            """Computes and returns grads as eager tensors."""

            with tf.GradientTape(persistent=gradients_fn is not None) as tape:
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

        def _lazy_tensor_dict(self, postprocessed_batch: SampleBatch):
            # TODO: (sven): Keep for a while to ensure backward compatibility.
            if not isinstance(postprocessed_batch, SampleBatch):
                postprocessed_batch = SampleBatch(postprocessed_batch)
            postprocessed_batch.set_get_interceptor(_convert_to_tf)
            return postprocessed_batch

        @classmethod
        def with_tracing(cls):
            return traced_eager_policy(cls)

    eager_policy_cls.__name__ = name + "_eager"
    eager_policy_cls.__qualname__ = name + "_eager"
    return eager_policy_cls
