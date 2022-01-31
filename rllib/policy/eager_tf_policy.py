"""Eager mode TF policy built using build_tf_policy().

It supports both traced and non-traced eager execution modes."""

import functools
import logging
import threading
import tree  # pip install dm_tree
from typing import Dict, List, Optional, Tuple

from ray.util.debug import log_once
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_TRAINED
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import normalize_action
from ray.rllib.utils.tf_utils import get_gpu_devices
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.typing import LocalOptimizer, ModelGradients, TensorType

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


def _convert_to_tf(x, dtype=None):
    if isinstance(x, SampleBatch):
        dict_ = {k: v for k, v in x.items() if k != SampleBatch.INFOS}
        return tf.nest.map_structure(_convert_to_tf, dict_)
    elif isinstance(x, Policy):
        return x
    # Special handling of "Repeated" values.
    elif isinstance(x, RepeatedValues):
        return RepeatedValues(
            tf.nest.map_structure(_convert_to_tf, x.values), x.lengths, x.max_len
        )

    if x is not None:
        d = dtype
        return tf.nest.map_structure(
            lambda f: _convert_to_tf(f, d)
            if isinstance(f, RepeatedValues)
            else tf.convert_to_tensor(f, d)
            if f is not None and not tf.is_tensor(f)
            else f,
            x,
        )

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
            ("Object of type {} has no method to convert to numpy.").format(type(x))
        )


def convert_eager_inputs(func):
    @functools.wraps(func)
    def _func(*args, **kwargs):
        if tf.executing_eagerly():
            eager_args = [_convert_to_tf(x) for x in args]
            # TODO: (sven) find a way to remove key-specific hacks.
            eager_kwargs = {
                k: _convert_to_tf(v, dtype=tf.int64 if k == "timestep" else None)
                for k, v in kwargs.items()
                if k not in {"info_batch", "episodes"}
            }
            return func(*eager_args, **eager_kwargs)
        else:
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
    raise ValueError(
        "Detected a variable being created during an eager "
        "forward pass. Variables should only be created during "
        "model initialization: {}".format(v.name)
    )


def check_too_many_retraces(obj):
    """Asserts that a given number of re-traces is not breached."""

    def _func(self_, *args, **kwargs):
        if (
            self_.config.get("eager_max_retraces") is not None
            and self_._re_trace_counter > self_.config["eager_max_retraces"]
        ):
            raise RuntimeError(
                "Too many tf-eager re-traces detected! This could lead to"
                " significant slow-downs (even slower than running in "
                "tf-eager mode w/ `eager_tracing=False`). To switch off "
                "these re-trace counting checks, set `eager_max_retraces`"
                " in your config to None."
            )
        return obj(self_, *args, **kwargs)

    return _func


def traced_eager_policy(eager_policy_cls):
    """Wrapper class that enables tracing for all eager policy methods.

    This is enabled by the `--trace`/`eager_tracing=True` config when
    framework=[tf2|tfe].
    """

    class TracedEagerPolicy(eager_policy_cls):
        def __init__(self, *args, **kwargs):
            self._traced_learn_on_batch_helper = False
            self._traced_compute_actions_helper = False
            self._traced_compute_gradients_helper = False
            self._traced_apply_gradients_helper = False
            super(TracedEagerPolicy, self).__init__(*args, **kwargs)

        @check_too_many_retraces
        @override(Policy)
        def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            episodes: Optional[List[Episode]] = None,
            **kwargs,
        ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
            """Traced version of Policy.compute_actions_from_input_dict."""

            # Create a traced version of `self._compute_actions_helper`.
            if self._traced_compute_actions_helper is False and not self._no_tracing:
                self._compute_actions_helper = convert_eager_inputs(
                    tf.function(
                        super(TracedEagerPolicy, self)._compute_actions_helper,
                        autograph=False,
                        experimental_relax_shapes=True,
                    )
                )
                self._traced_compute_actions_helper = True

            # Now that the helper method is traced, call super's
            # apply_gradients (which will call the traced helper).
            return super(TracedEagerPolicy, self).compute_actions_from_input_dict(
                input_dict=input_dict,
                explore=explore,
                timestep=timestep,
                episodes=episodes,
                **kwargs,
            )

        @check_too_many_retraces
        @override(eager_policy_cls)
        def learn_on_batch(self, samples):
            """Traced version of Policy.learn_on_batch."""

            # Create a traced version of `self._learn_on_batch_helper`.
            if self._traced_learn_on_batch_helper is False and not self._no_tracing:
                self._learn_on_batch_helper = convert_eager_inputs(
                    tf.function(
                        super(TracedEagerPolicy, self)._learn_on_batch_helper,
                        autograph=False,
                        experimental_relax_shapes=True,
                    )
                )
                self._traced_learn_on_batch_helper = True

            # Now that the helper method is traced, call super's
            # apply_gradients (which will call the traced helper).
            return super(TracedEagerPolicy, self).learn_on_batch(samples)

        @check_too_many_retraces
        @override(eager_policy_cls)
        def compute_gradients(self, samples: SampleBatch) -> ModelGradients:
            """Traced version of Policy.compute_gradients."""

            # Create a traced version of `self._compute_gradients_helper`.
            if self._traced_compute_gradients_helper is False and not self._no_tracing:
                self._compute_gradients_helper = convert_eager_inputs(
                    tf.function(
                        super(TracedEagerPolicy, self)._compute_gradients_helper,
                        autograph=False,
                        experimental_relax_shapes=True,
                    )
                )
                self._traced_compute_gradients_helper = True

            # Now that the helper method is traced, call super's
            # apply_gradients (which will call the traced helper).
            return super(TracedEagerPolicy, self).compute_gradients(samples)

        @check_too_many_retraces
        @override(Policy)
        def apply_gradients(self, grads: ModelGradients) -> None:
            """Traced version of Policy.apply_gradients."""

            # Create a traced version of `self._apply_gradients_helper`.
            if self._traced_apply_gradients_helper is False and not self._no_tracing:
                self._apply_gradients_helper = convert_eager_inputs(
                    tf.function(
                        super(TracedEagerPolicy, self)._apply_gradients_helper,
                        autograph=False,
                        experimental_relax_shapes=True,
                    )
                )
                self._traced_apply_gradients_helper = True

            # Now that the helper method is traced, call super's
            # apply_gradients (which will call the traced helper).
            return super(TracedEagerPolicy, self).apply_gradients(grads)

    TracedEagerPolicy.__name__ = eager_policy_cls.__name__ + "_traced"
    TracedEagerPolicy.__qualname__ = eager_policy_cls.__qualname__ + "_traced"
    return TracedEagerPolicy


class OptimizerWrapper:
    def __init__(self, tape):
        self.tape = tape

    def compute_gradients(self, loss, var_list):
        return list(zip(self.tape.gradient(loss, var_list), var_list))


def build_eager_tf_policy(
    name,
    loss_fn,
    get_default_config=None,
    postprocess_fn=None,
    stats_fn=None,
    optimizer_fn=None,
    compute_gradients_fn=None,
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
    get_batch_divisibility_req=None,
    # Deprecated args.
    obs_include_prev_action_reward=DEPRECATED_VALUE,
    extra_action_fetches_fn=None,
    gradients_fn=None,
):
    """Build an eager TF policy.

    An eager policy runs all operations in eager mode, which makes debugging
    much simpler, but has lower performance.

    You shouldn't need to call this directly. Rather, prefer to build a TF
    graph policy and use set {"framework": "tfe"} in the trainer config to have
    it automatically be converted to an eager policy.

    This has the same signature as build_tf_policy()."""

    base = add_mixins(Policy, mixins)

    if obs_include_prev_action_reward != DEPRECATED_VALUE:
        deprecation_warning(old="obs_include_prev_action_reward", error=False)

    if extra_action_fetches_fn is not None:
        deprecation_warning(
            old="extra_action_fetches_fn", new="extra_action_out_fn", error=False
        )
        extra_action_out_fn = extra_action_fetches_fn

    if gradients_fn is not None:
        deprecation_warning(old="gradients_fn", new="compute_gradients_fn", error=False)
        compute_gradients_fn = gradients_fn

    class eager_policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            # If this class runs as a @ray.remote actor, eager mode may not
            # have been activated yet.
            if not tf1.executing_eagerly():
                tf1.enable_eager_execution()
            self.framework = config.get("framework", "tfe")
            Policy.__init__(self, observation_space, action_space, config)

            # Log device and worker index.
            from ray.rllib.evaluation.rollout_worker import get_global_worker

            worker = get_global_worker()
            worker_idx = worker.worker_index if worker else 0
            if get_gpu_devices():
                logger.info(
                    "TF-eager Policy (worker={}) running on GPU.".format(
                        worker_idx if worker_idx > 0 else "local"
                    )
                )
            else:
                logger.info(
                    "TF-eager Policy (worker={}) running on CPU.".format(
                        worker_idx if worker_idx > 0 else "local"
                    )
                )

            self._is_training = False

            # Only for `config.eager_tracing=True`: A counter to keep track of
            # how many times an eager-traced method (e.g.
            # `self._compute_actions_helper`) has been re-traced by tensorflow.
            # We will raise an error if more than n re-tracings have been
            # detected, since this would considerably slow down execution.
            # The variable below should only get incremented during the
            # tf.function trace operations, never when calling the already
            # traced function after that.
            self._re_trace_counter = 0

            self._loss_initialized = False
            # To ensure backward compatibility:
            # Old way: If `loss` provided here, use as-is (as a function).
            if loss_fn is not None:
                self._loss = loss_fn
            # New way: Convert the overridden `self.loss` into a plain
            # function, so it can be called the same way as `loss` would
            # be, ensuring backward compatibility.
            elif self.loss.__func__.__qualname__ != "Policy.loss":
                self._loss = self.loss.__func__
            # `loss` not provided nor overridden from Policy -> Set to None.
            else:
                self._loss = None

            self.batch_divisibility_req = (
                get_batch_divisibility_req(self)
                if callable(get_batch_divisibility_req)
                else (get_batch_divisibility_req or 1)
            )
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
                        "`action_distribution_fn` is given"
                    )
            else:
                self.dist_class, logit_dim = ModelCatalog.get_action_dist(
                    action_space, self.config["model"]
                )

            if make_model:
                self.model = make_model(self, observation_space, action_space, config)
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
                optimizers = self.exploration.get_exploration_optimizer(optimizers)

            # The list of local (tf) optimizers (one per loss term).
            self._optimizers: List[LocalOptimizer] = optimizers
            # Backward compatibility: A user's policy may only support a single
            # loss term and optimizer (no lists).
            self._optimizer: LocalOptimizer = optimizers[0] if optimizers else None

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
        def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            episodes: Optional[List[Episode]] = None,
            **kwargs,
        ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

            if not self.config.get("eager_tracing") and not tf1.executing_eagerly():
                tf1.enable_eager_execution()

            self._is_training = False

            explore = explore if explore is not None else self.config["explore"]
            timestep = timestep if timestep is not None else self.global_timestep
            if isinstance(timestep, tf.Tensor):
                timestep = int(timestep.numpy())

            # Pass lazy (eager) tensor dict to Model as `input_dict`.
            input_dict = self._lazy_tensor_dict(input_dict)
            input_dict.set_training(False)

            # Pack internal state inputs into (separate) list.
            state_batches = [
                input_dict[k] for k in input_dict.keys() if "state_in" in k[:8]
            ]
            self._state_in = state_batches
            self._is_recurrent = state_batches != []

            # Call the exploration before_compute_actions hook.
            self.exploration.before_compute_actions(
                timestep=timestep, explore=explore, tf_sess=self.get_session()
            )

            ret = self._compute_actions_helper(
                input_dict,
                state_batches,
                # TODO: Passing episodes into a traced method does not work.
                None if self.config["eager_tracing"] else episodes,
                explore,
                timestep,
            )
            # Update our global timestep by the batch size.
            self.global_timestep += int(tree.flatten(ret[0])[0].shape[0])
            return convert_to_numpy(ret)

        @override(Policy)
        def compute_actions(
            self,
            obs_batch,
            state_batches=None,
            prev_action_batch=None,
            prev_reward_batch=None,
            info_batch=None,
            episodes=None,
            explore=None,
            timestep=None,
            **kwargs,
        ):

            # Create input dict to simply pass the entire call to
            # self.compute_actions_from_input_dict().
            input_dict = SampleBatch(
                {
                    SampleBatch.CUR_OBS: obs_batch,
                },
                _is_training=tf.constant(False),
            )
            if state_batches is not None:
                for s in enumerate(state_batches):
                    input_dict["state_in_{i}"] = s
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
            if info_batch is not None:
                input_dict[SampleBatch.INFOS] = info_batch

            return self.compute_actions_from_input_dict(
                input_dict=input_dict,
                explore=explore,
                timestep=timestep,
                episodes=episodes,
                **kwargs,
            )

        @with_lock
        @override(Policy)
        def compute_log_likelihoods(
            self,
            actions,
            obs_batch,
            state_batches=None,
            prev_action_batch=None,
            prev_reward_batch=None,
            actions_normalized=True,
        ):
            if action_sampler_fn and action_distribution_fn is None:
                raise ValueError(
                    "Cannot compute log-prob/likelihood w/o an "
                    "`action_distribution_fn` and a provided "
                    "`action_sampler_fn`!"
                )

            seq_lens = tf.ones(len(obs_batch), dtype=tf.int32)
            input_batch = SampleBatch(
                {SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_batch)},
                _is_training=False,
            )
            if prev_action_batch is not None:
                input_batch[SampleBatch.PREV_ACTIONS] = tf.convert_to_tensor(
                    prev_action_batch
                )
            if prev_reward_batch is not None:
                input_batch[SampleBatch.PREV_REWARDS] = tf.convert_to_tensor(
                    prev_reward_batch
                )

            # Exploration hook before each forward pass.
            self.exploration.before_compute_actions(explore=False)

            # Action dist class and inputs are generated via custom function.
            if action_distribution_fn:
                dist_inputs, dist_class, _ = action_distribution_fn(
                    self, self.model, input_batch, explore=False, is_training=False
                )
            # Default log-likelihood calculation.
            else:
                dist_inputs, _ = self.model(input_batch, state_batches, seq_lens)
                dist_class = self.dist_class

            action_dist = dist_class(dist_inputs, self.model)

            # Normalize actions if necessary.
            if not actions_normalized and self.config["normalize_actions"]:
                actions = normalize_action(actions, self.action_space_struct)

            log_likelihoods = action_dist.logp(actions)

            return log_likelihoods

        @override(Policy)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            assert tf.executing_eagerly()
            # Call super's postprocess_trajectory first.
            sample_batch = Policy.postprocess_trajectory(self, sample_batch)
            if postprocess_fn:
                return postprocess_fn(self, sample_batch, other_agent_batches, episode)
            return sample_batch

        @with_lock
        @override(Policy)
        def learn_on_batch(self, postprocessed_batch):
            # Callback handling.
            learn_stats = {}
            self.callbacks.on_learn_on_batch(
                policy=self, train_batch=postprocessed_batch, result=learn_stats
            )

            pad_batch_to_sequences_of_same_size(
                postprocessed_batch,
                max_seq_len=self._max_seq_len,
                shuffle=False,
                batch_divisibility_req=self.batch_divisibility_req,
                view_requirements=self.view_requirements,
            )

            self._is_training = True
            postprocessed_batch = self._lazy_tensor_dict(postprocessed_batch)
            postprocessed_batch.set_training(True)
            stats = self._learn_on_batch_helper(postprocessed_batch)
            stats.update(
                {
                    "custom_metrics": learn_stats,
                    NUM_AGENT_STEPS_TRAINED: postprocessed_batch.count,
                }
            )
            return convert_to_numpy(stats)

        @override(Policy)
        def compute_gradients(
            self, postprocessed_batch: SampleBatch
        ) -> Tuple[ModelGradients, Dict[str, TensorType]]:

            pad_batch_to_sequences_of_same_size(
                postprocessed_batch,
                shuffle=False,
                max_seq_len=self._max_seq_len,
                batch_divisibility_req=self.batch_divisibility_req,
                view_requirements=self.view_requirements,
            )

            self._is_training = True
            self._lazy_tensor_dict(postprocessed_batch)
            postprocessed_batch.set_training(True)
            grads_and_vars, grads, stats = self._compute_gradients_helper(
                postprocessed_batch
            )
            return convert_to_numpy((grads, stats))

        @override(Policy)
        def apply_gradients(self, gradients: ModelGradients) -> None:
            self._apply_gradients_helper(
                list(
                    zip(
                        [
                            (tf.convert_to_tensor(g) if g is not None else None)
                            for g in gradients
                        ],
                        self.model.trainable_variables(),
                    )
                )
            )

        @override(Policy)
        def get_weights(self, as_dict=False):
            variables = self.variables()
            if as_dict:
                return {v.name: v.numpy() for v in variables}
            return [v.numpy() for v in variables]

        @override(Policy)
        def set_weights(self, weights):
            variables = self.variables()
            assert len(weights) == len(variables), (len(weights), len(variables))
            for v, w in zip(variables, weights):
                v.assign(w)

        @override(Policy)
        def get_exploration_state(self):
            return convert_to_numpy(self.exploration.get_state())

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

        @override(Policy)
        def get_state(self):
            state = super().get_state()
            if self._optimizer and len(self._optimizer.variables()) > 0:
                state["_optimizer_variables"] = self._optimizer.variables()
            # Add exploration state.
            state["_exploration_state"] = self.exploration.get_state()
            return state

        @override(Policy)
        def set_state(self, state):
            state = state.copy()  # shallow copy
            # Set optimizer vars first.
            optimizer_vars = state.get("_optimizer_variables", None)
            if optimizer_vars and self._optimizer.variables():
                logger.warning(
                    "Cannot restore an optimizer's state for tf eager! Keras "
                    "is not able to save the v1.x optimizers (from "
                    "tf.compat.v1.train) since they aren't compatible with "
                    "checkpoints."
                )
                for opt_var, value in zip(self._optimizer.variables(), optimizer_vars):
                    opt_var.assign(value)
            # Set exploration's state.
            if hasattr(self, "exploration") and "_exploration_state" in state:
                self.exploration.set_state(state=state["_exploration_state"])
            # Then the Policy's (NN) weights.
            super().set_state(state)

        @override(Policy)
        def export_checkpoint(self, export_dir):
            raise NotImplementedError  # TODO: implement this

        @override(Policy)
        def export_model(self, export_dir):
            raise NotImplementedError  # TODO: implement this

        def variables(self):
            """Return the list of all savable variables for this policy."""
            if isinstance(self.model, tf.keras.Model):
                return self.model.variables
            else:
                return self.model.variables()

        def loss_initialized(self):
            return self._loss_initialized

        @with_lock
        def _compute_actions_helper(
            self, input_dict, state_batches, episodes, explore, timestep
        ):
            # Increase the tracing counter to make sure we don't re-trace too
            # often. If eager_tracing=True, this counter should only get
            # incremented during the @tf.function trace operations, never when
            # calling the already traced function after that.
            self._re_trace_counter += 1

            # Calculate RNN sequence lengths.
            batch_size = tree.flatten(input_dict[SampleBatch.OBS])[0].shape[0]
            seq_lens = tf.ones(batch_size, dtype=tf.int32) if state_batches else None

            # Add default and custom fetches.
            extra_fetches = {}

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
                        episodes=episodes,
                    )
                else:
                    if action_distribution_fn:

                        # Try new action_distribution_fn signature, supporting
                        # state_batches and seq_lens.
                        try:
                            (
                                dist_inputs,
                                self.dist_class,
                                state_out,
                            ) = action_distribution_fn(
                                self,
                                self.model,
                                input_dict=input_dict,
                                state_batches=state_batches,
                                seq_lens=seq_lens,
                                explore=explore,
                                timestep=timestep,
                                is_training=False,
                            )
                        # Trying the old way (to stay backward compatible).
                        # TODO: Remove in future.
                        except TypeError as e:
                            if (
                                "positional argument" in e.args[0]
                                or "unexpected keyword argument" in e.args[0]
                            ):
                                (
                                    dist_inputs,
                                    self.dist_class,
                                    state_out,
                                ) = action_distribution_fn(
                                    self,
                                    self.model,
                                    input_dict[SampleBatch.OBS],
                                    explore=explore,
                                    timestep=timestep,
                                    is_training=False,
                                )
                            else:
                                raise e
                    elif isinstance(self.model, tf.keras.Model):
                        input_dict = SampleBatch(input_dict, seq_lens=seq_lens)
                        if state_batches and "state_in_0" not in input_dict:
                            for i, s in enumerate(state_batches):
                                input_dict[f"state_in_{i}"] = s
                        self._lazy_tensor_dict(input_dict)
                        dist_inputs, state_out, extra_fetches = self.model(input_dict)
                    else:
                        dist_inputs, state_out = self.model(
                            input_dict, state_batches, seq_lens
                        )

                    action_dist = self.dist_class(dist_inputs, self.model)

                    # Get the exploration action from the forward results.
                    actions, logp = self.exploration.get_exploration_action(
                        action_distribution=action_dist,
                        timestep=timestep,
                        explore=explore,
                    )

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

            return actions, state_out, extra_fetches

        def _learn_on_batch_helper(self, samples):
            # Increase the tracing counter to make sure we don't re-trace too
            # often. If eager_tracing=True, this counter should only get
            # incremented during the @tf.function trace operations, never when
            # calling the already traced function after that.
            self._re_trace_counter += 1

            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, _, stats = self._compute_gradients_helper(samples)
            self._apply_gradients_helper(grads_and_vars)
            return stats

        def _get_is_training_placeholder(self):
            return tf.convert_to_tensor(self._is_training)

        @with_lock
        def _compute_gradients_helper(self, samples):
            """Computes and returns grads as eager tensors."""

            # Increase the tracing counter to make sure we don't re-trace too
            # often. If eager_tracing=True, this counter should only get
            # incremented during the @tf.function trace operations, never when
            # calling the already traced function after that.
            self._re_trace_counter += 1

            # Gather all variables for which to calculate losses.
            if isinstance(self.model, tf.keras.Model):
                variables = self.model.trainable_variables
            else:
                variables = self.model.trainable_variables()

            # Calculate the loss(es) inside a tf GradientTape.
            with tf.GradientTape(persistent=compute_gradients_fn is not None) as tape:
                losses = self._loss(self, self.model, self.dist_class, samples)
            losses = force_list(losses)

            # User provided a compute_gradients_fn.
            if compute_gradients_fn:
                # Wrap our tape inside a wrapper, such that the resulting
                # object looks like a "classic" tf.optimizer. This way, custom
                # compute_gradients_fn will work on both tf static graph
                # and tf-eager.
                optimizer = OptimizerWrapper(tape)
                # More than one loss terms/optimizers.
                if self.config["_tf_policy_handles_more_than_one_loss"]:
                    grads_and_vars = compute_gradients_fn(
                        self, [optimizer] * len(losses), losses
                    )
                # Only one loss and one optimizer.
                else:
                    grads_and_vars = [compute_gradients_fn(self, optimizer, losses[0])]
            # Default: Compute gradients using the above tape.
            else:
                grads_and_vars = [
                    list(zip(tape.gradient(loss, variables), variables))
                    for loss in losses
                ]

            if log_once("grad_vars"):
                for g_and_v in grads_and_vars:
                    for g, v in g_and_v:
                        if g is not None:
                            logger.info(f"Optimizing variable {v.name}")

            # `grads_and_vars` is returned a list (len=num optimizers/losses)
            # of lists of (grad, var) tuples.
            if self.config["_tf_policy_handles_more_than_one_loss"]:
                grads = [[g for g, _ in g_and_v] for g_and_v in grads_and_vars]
            # `grads_and_vars` is returned as a list of (grad, var) tuples.
            else:
                grads_and_vars = grads_and_vars[0]
                grads = [g for g, _ in grads_and_vars]

            stats = self._stats(self, samples, grads)
            return grads_and_vars, grads, stats

        def _apply_gradients_helper(self, grads_and_vars):
            # Increase the tracing counter to make sure we don't re-trace too
            # often. If eager_tracing=True, this counter should only get
            # incremented during the @tf.function trace operations, never when
            # calling the already traced function after that.
            self._re_trace_counter += 1

            if apply_gradients_fn:
                if self.config["_tf_policy_handles_more_than_one_loss"]:
                    apply_gradients_fn(self, self._optimizers, grads_and_vars)
                else:
                    apply_gradients_fn(self, self._optimizer, grads_and_vars)
            else:
                if self.config["_tf_policy_handles_more_than_one_loss"]:
                    for i, o in enumerate(self._optimizers):
                        o.apply_gradients(
                            [(g, v) for g, v in grads_and_vars[i] if g is not None]
                        )
                else:
                    self._optimizer.apply_gradients(
                        [(g, v) for g, v in grads_and_vars if g is not None]
                    )

        def _stats(self, outputs, samples, grads):

            fetches = {}
            if stats_fn:
                fetches[LEARNER_STATS_KEY] = {
                    k: v for k, v in stats_fn(outputs, samples).items()
                }
            else:
                fetches[LEARNER_STATS_KEY] = {}

            if extra_learn_fetches_fn:
                fetches.update({k: v for k, v in extra_learn_fetches_fn(self).items()})
            if grad_stats_fn:
                fetches.update(
                    {k: v for k, v in grad_stats_fn(self, samples, grads).items()}
                )
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
