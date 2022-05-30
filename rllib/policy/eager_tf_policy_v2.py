"""Eager mode TF policy built using build_tf_policy().

It supports both traced and non-traced eager execution modes.
"""

import gym
import logging
import threading
import tree  # pip install dm_tree
from typing import Dict, List, Optional, Tuple, Type, Union

from ray.util.debug import log_once
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.eager_tf_policy import (
    _convert_to_tf,
    _disallow_var_creation,
    _OptimizerWrapper,
    _traced_eager_policy,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    is_overridden,
    override,
)
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_TRAINED
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import normalize_action
from ray.rllib.utils.tf_utils import get_gpu_devices
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorStructType,
    TensorType,
    TrainerConfigDict,
)

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


@DeveloperAPI
class EagerTFPolicyV2(Policy):
    """A TF-eageer / TF2 based tensorflow policy.

    This class is intended to be used and extended by sub-classing.
    """

    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
        **kwargs,
    ):
        self.framework = config.get("framework", "tf2")

        # Log device.
        logger.info(
            "Creating TF-eager policy running on {}.".format(
                "GPU" if get_gpu_devices() else "CPU"
            )
        )

        Policy.__init__(self, observation_space, action_space, config)

        config = dict(self.get_default_config(), **config)
        self.config = config

        self._is_training = False
        # Global timestep should be a tensor.
        self.global_timestep = tf.Variable(0, trainable=False, dtype=tf.int64)
        self.explore = tf.Variable(
            self.config["explore"], trainable=False, dtype=tf.bool
        )

        self._loss_initialized = False
        # Backward compatibility workaround so Policy will call self.loss() directly.
        # TODO(jungong): clean up after all policies are migrated to new sub-class
        # implementation.
        self._loss = None

        self.batch_divisibility_req = self.get_batch_divisibility_req()
        self._max_seq_len = config["model"]["max_seq_len"]

        self.validate_spaces(observation_space, action_space, config)

        # If using default make_model(), dist_class will get updated when
        # the model is created next.
        self.dist_class = self._init_dist_class()
        self.model = self.make_model()

        self._init_view_requirements()

        self.exploration = self._create_exploration()
        self._state_inputs = self.model.get_initial_state()
        self._is_recurrent = len(self._state_inputs) > 0

        # Got to reset global_timestep again after fake run-throughs.
        self.global_timestep.assign(0)

        # Lock used for locking some methods on the object-level.
        # This prevents possible race conditions when calling the model
        # first, then its value function (e.g. in a loss function), in
        # between of which another model call is made (e.g. to compute an
        # action).
        self._lock = threading.RLock()

        # Only for `config.eager_tracing=True`: A counter to keep track of
        # how many times an eager-traced method (e.g.
        # `self._compute_actions_helper`) has been re-traced by tensorflow.
        # We will raise an error if more than n re-tracings have been
        # detected, since this would considerably slow down execution.
        # The variable below should only get incremented during the
        # tf.function trace operations, never when calling the already
        # traced function after that.
        self._re_trace_counter = 0

    @DeveloperAPI
    @staticmethod
    def enable_eager_execution_if_necessary():
        # If this class runs as a @ray.remote actor, eager mode may not
        # have been activated yet.
        if tf1 and not tf1.executing_eagerly():
            tf1.enable_eager_execution()

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def get_default_config(self) -> TrainerConfigDict:
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def validate_spaces(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):
        return {}

    @override(Policy)
    def compute_actions(
        self,
        *,
        input_dict: Optional[Union[SampleBatch, Dict[str, TensorStructType]]] = None,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        episodes: Optional[List["Episode"]] = None,
        is_training: bool = False,
        # Deprecated args.
        obs_batch=None,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        info_batch=None,
        # Kwargs for forward compatibility.
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        # If old signature used -> Warning and move everything into `input_dict`.
        if input_dict is None:
            assert obs_batch is not None, (
                "ERROR: `compute_actions` either takes `input_dict` OR `obs_batch` as"
                " arg! You didn't provide either."
            )
            if log_once("old_compute_actions_signature"):
                deprecation_warning(
                    old="compute_actions(obs_batch=[..])",
                    new="compute_actions(input_dict=[..])",
                    error=False,
                )
            # Create `input_dict` to continue with.
            input_dict = SampleBatch(
                {
                    SampleBatch.CUR_OBS: obs_batch,
                },
                _is_training=tf.constant(False),
            )
            if state_batches is not None:
                for i, s in enumerate(state_batches):
                    input_dict[f"state_in_{i}"] = s
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
            if info_batch is not None:
                input_dict[SampleBatch.INFOS] = info_batch
        else:
            # Pack internal state inputs into (separate) list.
            state_key = "state_in_0"
            state_batches = []
            i = 0
            while state_key in input_dict:
                i += 1
                state_batches.append(input_dict[state_key])
                state_key = f"state_in_{i}"

        self._is_recurrent = state_batches != []
        self._is_training = False

        explore = explore if explore is not None else self.explore
        timestep = timestep if timestep is not None else self.global_timestep
        if isinstance(timestep, tf.Tensor):
            timestep = int(timestep.numpy())

        # Pass lazy (eager) tensor dict to Model as `input_dict`.
        input_dict = self._lazy_tensor_dict(input_dict)
        input_dict.set_training(False)

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
        return ret

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
        if is_overridden(self.action_sampler_fn) and not is_overridden(
            self.action_distribution_fn
        ):
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
        if is_overridden(self.action_distribution_fn):
            dist_inputs, dist_class, _ = self.action_distribution_fn(
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

    def initialize_optimizer_and_loss_if_necessary(self):
        optimizers = force_list(self.make_optimizer())
        if getattr(self, "exploration", None):
            optimizers = self.exploration.get_exploration_optimizer(optimizers)

        # The list of local (tf) optimizers (one per loss term).
        self._optimizers: List[LocalOptimizer] = optimizers
        # Backward compatibility: A user's policy may only support a single
        # loss term and optimizer (no lists).
        self._optimizer: LocalOptimizer = optimizers[0] if optimizers else None

        self._initialize_loss_from_dummy_batch(
            auto_remove_unneeded_view_reqs=True,
        )
        self._loss_initialized = True

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    @override(Policy)
    def loss(
        self,
        model: Union[ModelV2, "tf.keras.Model"],
        dist_class: Type[TFActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Compute loss for this policy using model, dist_class and a train_batch.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            A single loss tensor or a list of loss tensors.
        """
        raise NotImplementedError

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def compute_gradients_fn(
        self, policy: Policy, optimizer: LocalOptimizer, loss: TensorType
    ) -> ModelGradients:
        """Gradients computing function (from loss tensor, using local optimizer).

        Args:
            policy (Policy): The Policy object that generated the loss tensor and
                that holds the given local optimizer.
            optimizer (LocalOptimizer): The tf (local) optimizer object to
                calculate the gradients with.
            loss (TensorType): The loss tensor for which gradients should be
                calculated.

        Returns:
            ModelGradients: List of the possibly clipped gradients- and variable
                tuples.
        """
        return None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def apply_gradients_fn(
        self,
        policy: Policy,
        optimizer: "tf.keras.optimizers.Optimizer",
        grads: ModelGradients,
    ) -> "tf.Operation":
        """Gradients computing function (from loss tensor, using local optimizer).

        Args:
            policy (Policy): The Policy object that generated the loss tensor and
                that holds the given local optimizer.
            optimizer (LocalOptimizer): The tf (local) optimizer object to
                calculate the gradients with.
            grads (ModelGradients): The gradient tensor to be applied.

        Returns:
            "tf.Operation": TF operation that applies supplied gradients.
        """
        return None

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def grad_stats_fn(
        self, train_batch: SampleBatch, grads: ModelGradients
    ) -> Dict[str, TensorType]:
        """Gradient stats function. Returns a dict of statistics.

        Args:
            train_batch: The SampleBatch (already) used for training.

        Returns:
            The stats dict.
        """
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def get_batch_divisibility_req(self) -> int:
        """Get batch divisibility request.

        Returns:
            Size N. A sample batch must be of size K*N.
        """
        # By default, any sized batch is ok, so simply return 1.
        return 1

    @DeveloperAPI
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def extra_learn_fetches_fn(self) -> Dict[str, TensorType]:
        """Extra stats to be reported after gradient computation.

        Returns:
             Dict[str, TensorType]: An extra fetch-dict.
        """
        return {}

    @OverrideToImplementCustomLogic
    def make_optimizer(
        self,
    ) -> Union["tf.keras.optimizers.Optimizer", List["tf.keras.optimizers.Optimizer"]]:
        """TF optimizer to use for policy optimization.

        Returns:
            A local optimizer or a list of local optimizers to use for this
                Policy's Model.
        """
        return tf.keras.optimizers.Adam(self.config["lr"])

    def _init_dist_class(self):
        dist_class, _ = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"]
        )
        return dist_class

    def _init_view_requirements(self):
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()
        # Combine view_requirements for Model and Policy.
        # TODO(jungong) : models will not carry view_requirements once they
        # are migrated to be organic Keras models.
        self.view_requirements.update(self.model.view_requirements)
        # Disable env-info placeholder.
        if SampleBatch.INFOS in self.view_requirements:
            self.view_requirements[SampleBatch.INFOS].used_for_training = False

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
        state["global_timestep"] = state["global_timestep"].numpy()
        if self._optimizer and len(self._optimizer.variables()) > 0:
            state["_optimizer_variables"] = self._optimizer.variables()
        # Add exploration state.
        state["_exploration_state"] = self.exploration.get_state()
        return state

    @override(Policy)
    def set_state(self, state):
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
        # Weights and global_timestep (tf vars).
        self.set_weights(state["weights"])
        self.global_timestep.assign(state["global_timestep"])

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

    # TODO: Figure out, why _ray_trace_ctx=None helps to prevent a crash in
    #  AlphaStar w/ framework=tf2; eager_tracing=True on the policy learner actors.
    #  It seems there may be a clash between the traced-by-tf function and the
    #  traced-by-ray functions (for making the policy class a ray actor).
    @with_lock
    def _compute_actions_helper(
        self,
        input_dict,
        state_batches,
        episodes,
        explore,
        timestep,
        _ray_trace_ctx=None,
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
            if isinstance(self.model, tf.keras.Model):
                input_dict = SampleBatch(input_dict, seq_lens=seq_lens)
                if state_batches and "state_in_0" not in input_dict:
                    for i, s in enumerate(state_batches):
                        input_dict[f"state_in_{i}"] = s
                self._lazy_tensor_dict(input_dict)
                dist_inputs, state_out, extra_fetches = self.model(input_dict)
            else:
                dist_inputs, state_out = self.model(input_dict, state_batches, seq_lens)

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

        return actions, state_out, extra_fetches

    # TODO: Figure out, why _ray_trace_ctx=None helps to prevent a crash in
    #  AlphaStar w/ framework=tf2; eager_tracing=True on the policy learner actors.
    #  It seems there may be a clash between the traced-by-tf function and the
    #  traced-by-ray functions (for making the policy class a ray actor).
    def _learn_on_batch_helper(self, samples, _ray_trace_ctx=None):
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
        with tf.GradientTape(
            persistent=is_overridden(self.compute_gradients_fn)
        ) as tape:
            losses = self.loss(self.model, self.dist_class, samples)
        losses = force_list(losses)

        # User provided a custom compute_gradients_fn.
        if is_overridden(self.compute_gradients_fn):
            # Wrap our tape inside a wrapper, such that the resulting
            # object looks like a "classic" tf.optimizer. This way, custom
            # compute_gradients_fn will work on both tf static graph
            # and tf-eager.
            optimizer = _OptimizerWrapper(tape)
            # More than one loss terms/optimizers.
            if self.config["_tf_policy_handles_more_than_one_loss"]:
                grads_and_vars = self.compute_gradients_fn(
                    [optimizer] * len(losses), losses
                )
            # Only one loss and one optimizer.
            else:
                grads_and_vars = [self.compute_gradients_fn(optimizer, losses[0])]
        # Default: Compute gradients using the above tape.
        else:
            grads_and_vars = [
                list(zip(tape.gradient(loss, variables), variables)) for loss in losses
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

        stats = self._stats(samples, grads)
        return grads_and_vars, grads, stats

    def _apply_gradients_helper(self, grads_and_vars):
        # Increase the tracing counter to make sure we don't re-trace too
        # often. If eager_tracing=True, this counter should only get
        # incremented during the @tf.function trace operations, never when
        # calling the already traced function after that.
        self._re_trace_counter += 1

        if is_overridden(self.apply_gradients_fn):
            if self.config["_tf_policy_handles_more_than_one_loss"]:
                self.apply_gradients_fn(self._optimizers, grads_and_vars)
            else:
                self.apply_gradients_fn(self._optimizer, grads_and_vars)
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

    def _stats(self, samples, grads):
        fetches = {}
        if is_overridden(self.stats_fn):
            fetches[LEARNER_STATS_KEY] = {
                k: v for k, v in self.stats_fn(samples).items()
            }
        else:
            fetches[LEARNER_STATS_KEY] = {}

        fetches.update({k: v for k, v in self.extra_learn_fetches_fn().items()})
        fetches.update({k: v for k, v in self.grad_stats_fn(samples, grads).items()})
        return fetches

    def _lazy_tensor_dict(self, postprocessed_batch: SampleBatch):
        # TODO: (sven): Keep for a while to ensure backward compatibility.
        if not isinstance(postprocessed_batch, SampleBatch):
            postprocessed_batch = SampleBatch(postprocessed_batch)
        postprocessed_batch.set_get_interceptor(_convert_to_tf)
        return postprocessed_batch

    @classmethod
    def with_tracing(cls):
        return _traced_eager_policy(cls)
