import abc
import logging
import numpy
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from ray.rllib.connectors.learner.learner_connector_pipeline import (
    LearnerConnectorPipeline,
)
from ray.rllib.core import ALL_MODULES, COMPONENT_METRICS_LOGGER
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils import unflatten_dict
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_TRAINED,
    DATASET_NUM_ITERS_TRAINED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED_LIFETIME,
    MODULE_TRAIN_BATCH_SIZE_MEAN,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.minibatch_utils import (
    MiniBatchCyclicIterator,
    MiniBatchDummyIterator,
    MiniBatchRayDataIterator,
)
from ray.rllib.utils.typing import (
    DeviceType,
    ModuleID,
    NamedParamDict,
    ResultDict,
    StateDict,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.learner.differentiable_learner_config import (
        DifferentiableLearnerConfig,
    )

logger = logging.getLogger(__name__)


class DifferentiableLearner(Checkpointable):
    """A differentiable `Learner` class enabling functional parameter updates.

    This class is part of RLlib's Meta Learning API and provides a differentiable
    `Learner`, allowing higher-order updates within the meta-learning loop.

    Unlike standard learners, this class operates on a provided `RLModule` reference
    instead of creating its own. Updated cloned module parameters are returned to the
    caller, facilitating further update processes.

    The `update`, `_update`, `compute_gradients`, and `apply_gradients` methods
    require, in addition to other arguments, a dictionary of cloned `RLModule`
    parameters for functional updates.
    """

    # The framework an instance uses.
    framework_str: str

    # The key for the total loss.
    TOTAL_LOSS_KEY: str = "total_loss"

    def __init__(
        self,
        *,
        config: "AlgorithmConfig",
        learner_config: "DifferentiableLearnerConfig",
        module: Optional[RLModule] = None,
        **kwargs,
    ) -> None:

        # An `AlgorithmConfig` used to access certain global configurations.
        self.config: "AlgorithmConfig" = config.copy(copy_frozen=False)
        # The specific configuration for the differentiable learner instance.
        self.learner_config: "DifferentiableLearnerConfig" = learner_config
        # The reference to the caller's module.
        self._module: Optional[MultiRLModule] = module
        # The reference to the caller's device.
        self._device: Optional[DeviceType] = None
        # A counter for functional weight updates.
        self._weights_seq_no: int = 0

        # Whether self.build has already been called.
        self._is_built: bool = False

        # The learner connector pipeline.
        self._learner_connector: Optional[LearnerConnectorPipeline] = None

        # The Learner's own MetricsLogger to be used to log RLlib's built-in metrics or
        # custom user-defined ones (e.g. custom loss values). When returning from an
        # `update_from_...()` method call, the Learner will do a `self.metrics.reduce()`
        # and return the resulting (reduced) dict.
        self.metrics: MetricsLogger = MetricsLogger()

        # In case of offline learning and multiple learners, each learner receives a
        # repeatable iterator that iterates over a split of the streamed data.
        self.iterator: MiniBatchRayDataIterator = None

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def build(self, device: Optional[DeviceType] = None) -> None:

        if self._is_built:
            logger.debug("DifferentiableLearner already built. Skipping built.")

        # If a dvice was passed, set the `DifferentiableLearner`'s device.
        if device:
            self._device = device

        # TODO (simon): Move the `build_learner_connector` to the
        # `DifferentiableLearnerConfig`.
        self._learner_connector = self.learner_config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
            device=self._device,
        )

        # This instance is now ready for use.
        self._is_built = True

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_gradients(
        self,
        loss_per_module: Dict[ModuleID, TensorType],
        params: Dict[ModuleID, NamedParamDict],
        **kwargs,
    ) -> Dict[ModuleID, NamedParamDict]:
        """Computes functional gradients based on the given losses.

        Note that this method requires computing gradients functionally,
        without relying on an optimizer. If an optimizer is needed, a
        differentiable optimizer from a third-party package must be used.

        Args:
            loss_per_module: Dict mapping module IDs to their individual total loss
                terms, computed by the individual `compute_loss_for_module()` calls.
                The overall total loss (sum of loss terms over all modules) is stored
                under `loss_per_module[ALL_MODULES]`.
            params: A dictionary containing cloned parameters for each module id.
            **kwargs: Forward compatibility kwargs.

        Returns:
            The gradients in the same (dict) format as `params`.
        """

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def apply_gradients(
        self,
        gradients: Dict[ModuleID, NamedParamDict],
        params: Dict[ModuleID, NamedParamDict],
    ) -> Dict[ModuleID, NamedParamDict]:
        """Applies given gradients functionally.

        Note that this method requires functional parameter updates,
        meaning modifications must not be performed in-place (e.g., via an
        optimizer or directly within the `MultiRLModule`).

        Args:
            gradients: A dictionary containing named gradients for each module id.
            params: A dictionary containing named parameters for each module id.

        Returns:
            The updated parameters in the same (dict) format as `params`.
        """

    @OverrideToImplementCustomLogic
    def should_module_be_updated(self, module_id, multi_agent_batch=None):
        """Returns whether a module should be updated or not based on `self.config`.

        Args:
            module_id: The ModuleID that we want to query on whether this module
                should be updated or not.
            multi_agent_batch: An optional MultiAgentBatch to possibly provide further
                information on the decision on whether the RLModule should be updated
                or not.
        """
        should_module_be_updated_fn = self.config.policies_to_train
        # If None, return True (by default, all modules should be updated).
        if should_module_be_updated_fn is None:
            return True
        # If collection given, return whether `module_id` is in that container.
        elif not callable(should_module_be_updated_fn):
            return module_id in set(should_module_be_updated_fn)

        return should_module_be_updated_fn(module_id, multi_agent_batch)

    @OverrideToImplementCustomLogic
    def compute_losses(
        self, *, fwd_out: Dict[str, Any], batch: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Computes the loss(es) for the module being optimized.

        This method must be overridden by MultiRLModule-specific Learners in order to
        define the specific loss computation logic. If the algorithm is single-agent,
        only `compute_loss_for_module()` should be overridden instead. If the algorithm
        uses independent multi-agent learning (default behavior for RLlib's multi-agent
        setups), also only `compute_loss_for_module()` should be overridden, but it will
        be called for each individual RLModule inside the MultiRLModule.
        For the functional update to work, no `forward` call should be made
        within this method, especially not a non-functional one. Instead, use
        the model outputs provided by `fwd_out`.

        Args:
            fwd_out: Output from a functional call to the `forward_train()` method of the
                underlying MultiRLModule (`self.module`) during training
                (`self.update()`).
            batch: The train batch that was used to compute `fwd_out`.

        Returns:
            A dictionary mapping module IDs to individual loss terms.
        """
        loss_per_module = {}
        for module_id in fwd_out:
            loss = self.compute_loss_for_module(
                module_id=module_id,
                # TODO (simon): Check, if this should be provided per
                # `DifferentiableLearnerConfig`.
                config=self.config.get_config_for_module(module_id),
                batch=batch[module_id],
                fwd_out=fwd_out[module_id],
            )
            loss_per_module[module_id] = loss

        return loss_per_module

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Computes the loss for a single module.

        Think of this as computing loss for a single agent. For multi-agent use-cases
        that require more complicated computation for loss, consider overriding the
        `compute_losses` method instead.

        Args:
            module_id: The id of the module.
            config: The AlgorithmConfig specific to the given `module_id`.
            batch: The train batch for this particular module.
            fwd_out: The output of the forward pass for this particular module.

        Returns:
            A single total loss tensor. If you have more than one optimizer on the
            provided `module_id` and would like to compute gradients separately using
            these different optimizers, simply add up the individual loss terms for
            each optimizer and return the sum. Also, for recording/logging any
            individual loss terms, you can use the `Learner.metrics.log_value(
            key=..., value=...)` or `DifferentiableLearner.metrics.log_dict()` APIs.
            See: :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger` for
            more information.
        """

    def update(
        self,
        params: Dict[ModuleID, NamedParamDict],
        training_data: TrainingData,
        *,
        _no_metrics_reduce: bool = False,
        **kwargs,
    ) -> Tuple[Dict[ModuleID, NamedParamDict], ResultDict]:
        """Make a functional update on provided parameters.

        You can use this method to take more than one backward pass on the batch. All
        configuration parameters for the iteration loop are set within the
        `learner_config`. Note, the same configuration will be used for all module ids
        in `MultiRLModule`.

        Args:
            params: A parameter dictionary holding named parameters for each module id.
                These parameters must be a clone of the module's original parameters to
                perform a functional update on them.
            training_data: A `TrainingData` instance containing the data or data iterators
                to be used in updating the given parameters in `param`.

        Returns:
            The functionally updated parameters in the (dict) format they were passed in
            and a `ResultDict` object produced by a call to `self.metrics.reduce()`. The
            returned dict may be arbitrarily nested and must have `Stats` objects at
            all its leafs, allowing components further downstream (i.e. a user of this
            Learner) to further reduce these results (for example over n parallel
            Learners).
        """
        self._check_is_built()

        # TODO (simon): Implement a `before_gradient_based_update`, if necessary.
        # Call `before_gradient_based_update` to allow for non-gradient based
        # preparations-, logging-, and update logic to happen.
        # self.before_gradient_based_update(timesteps=timesteps or {})

        training_data.validate()
        training_data.solve_refs()
        assert training_data.batches is None, "`training_data.batches` must be None!"

        self._weights_seq_no += 1

        batch_iter = self._create_iterator_if_necessary(
            training_data=training_data,
            num_total_minibatches=self.learner_config.num_total_minibatches,
            num_epochs=self.learner_config.num_epochs,
            minibatch_size=self.learner_config.minibatch_size,
            shuffle_batch_per_epoch=self.learner_config.shuffle_batch_per_epoch,
        )

        self.metrics.activate_tensor_mode()

        # Perform the actual looping through the minibatches or the given data iterator.
        for iteration, tensor_minibatch in enumerate(batch_iter):
            # Check the MultiAgentBatch, whether our RLModule contains all ModuleIDs
            # found in this batch. If not, throw an error.
            unknown_module_ids = set(tensor_minibatch.policy_batches.keys()) - set(
                self.module.keys()
            )
            if unknown_module_ids:
                raise ValueError(
                    f"Batch contains one or more ModuleIDs ({unknown_module_ids}) that "
                    f"are not in this Learner!"
                )

            # Make the actual in-graph/traced `_update` call. This should return
            # all tensor values (no numpy).
            fwd_out, loss_per_module, params, _ = self._update(
                # TODO (simon): Maybe filter ParamDict by module keys in batch.
                tensor_minibatch.policy_batches,
                params,
            )

            # TODO (sven): Maybe move this into loop above to get metrics more accuratcely
            #  cover the minibatch/epoch logic.
            # Log all timesteps (env, agent, modules) based on given episodes/batch.
            self._log_steps_trained_metrics(tensor_minibatch)

            self._set_slicing_by_batch_id(tensor_minibatch, value=False)

        if self.iterator:
            # Record the number of batches pulled from the dataset.
            self.metrics.log_value(
                (ALL_MODULES, DATASET_NUM_ITERS_TRAINED),
                iteration + 1,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                (ALL_MODULES, DATASET_NUM_ITERS_TRAINED_LIFETIME),
                iteration + 1,
                reduce="sum",
            )
        # Log all individual RLModules' loss terms and its registered optimizers'
        # current learning rates.
        # Note: We do this only once for the last of the minibatch updates, b/c the
        # window is only 1 anyways.
        for mid, loss in loss_per_module.items():
            self.metrics.log_value(
                key=(mid, self.TOTAL_LOSS_KEY),
                value=loss,
                window=1,
            )

        # Call `after_gradient_based_update` to allow for non-gradient based
        # cleanups-, logging-, and update logic to happen.
        # TODO (simon): Check, if this should stay here, when running multiple
        # gradient steps inside the iterator loop above (could be a complete epoch)
        # the target networks might need to be updated earlier.
        # self.after_gradient_based_update(timesteps=timesteps or {})
        self.metrics.deactivate_tensor_mode()

        # Reduce results across all minibatch update steps.
        if not _no_metrics_reduce:
            return params, loss_per_module, self.metrics.reduce()
        else:
            return params, loss_per_module, {}

    def _create_iterator_if_necessary(
        self,
        *,
        training_data: TrainingData,
        num_total_minibatches: int = 0,
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        **kwargs,
    ) -> Iterable:
        """Provides a batch iterator."""

        # Data iterator provided.
        if training_data.data_iterators:
            num_iters = kwargs.pop("num_iters", None)
            if num_iters is None:
                raise ValueError(
                    "Learner.update(data_iterators=..) requires `num_iters` kwarg!"
                )

            def _collate_fn(_batch: Dict[str, numpy.ndarray]) -> MultiAgentBatch:
                _batch = unflatten_dict(_batch)
                _batch = MultiAgentBatch(
                    {
                        module_id: SampleBatch(module_data)
                        for module_id, module_data in _batch.items()
                    },
                    env_steps=sum(
                        len(next(iter(module_data.values())))
                        for module_data in _batch.values()
                    ),
                )
                _batch = self._convert_batch_type(_batch, to_device=False)
                return self._set_slicing_by_batch_id(_batch, value=True)

            def _finalize_fn(batch: MultiAgentBatch) -> MultiAgentBatch:
                return self._convert_batch_type(batch, to_device=True, use_stream=True)

            if not self.iterator:
                # This iterator holds a `ray.data.DataIterator` and manages it state.
                self.iterator = MiniBatchRayDataIterator(
                    iterator=training_data.data_iterators[0],
                    collate_fn=_collate_fn,
                    finalize_fn=_finalize_fn,
                    minibatch_size=minibatch_size,
                    num_iters=num_iters,
                    **kwargs,
                )

            batch_iter = self.iterator
        else:
            batch = self._make_batch_if_necessary(training_data=training_data)
            assert batch is not None

            # TODO: Move this into LearnerConnector pipeline?
            # Filter out those RLModules from the final train batch that should not be
            # updated.
            for module_id in list(batch.policy_batches.keys()):
                if not self.should_module_be_updated(module_id, batch):
                    del batch.policy_batches[module_id]
            if not batch.policy_batches:
                return {}

            batch = self._set_slicing_by_batch_id(batch, value=True)

            if minibatch_size:
                batch_iter_cls = MiniBatchCyclicIterator
            elif num_epochs > 1:
                # `minibatch_size` was not set but `num_epochs` > 1.
                minibatch_size = batch.count
                # Note that there is no need to shuffle here, b/c we don't have
                # minibatches.
                batch_iter_cls = MiniBatchCyclicIterator
            else:
                # `minibatch_size` and `num_epochs` are not set by the user.
                batch_iter_cls = MiniBatchDummyIterator

            batch_iter = batch_iter_cls(
                batch,
                num_epochs=num_epochs,
                minibatch_size=minibatch_size,
                shuffle_batch_per_epoch=shuffle_batch_per_epoch and (num_epochs > 1),
                num_total_minibatches=num_total_minibatches,
            )
        return batch_iter

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def _update(
        self,
        batch: Dict[str, Any],
        params: Dict[ModuleID, NamedParamDict],
        **kwargs,
    ) -> Tuple[Any, Dict[ModuleID, NamedParamDict], Any, Any]:
        """Contains all logic for an in-graph/traceable functional update step.

        Framework specific subclasses must implement this method. This should include
        functional calls to the RLModule's `forward_train`, `compute_loss`,
        `compute_gradients`, `postprocess_gradients`, and `apply_gradients` methods
        and return a tuple with all the individual results as well as the functionally
        updated parameter dictionary.

        Args:
            batch: The train batch already converted to a Dict mapping str to (possibly
                nested) tensors.
            kwargs: Forward compatibility kwargs.

        Returns:
            A tuple consisting of:
                1) the output of a functional forward call to the RLModule using
                    `params`,
                2) the functionally updated parameters in the (dict) format passed in,
                3) the `loss_per_module` dictionary mapping module IDs to individual
                    loss tensors,
                4) a metrics dict mapping module IDs to metrics key/value pairs.

        """

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        """Gets the state of the `DifferentiableLearner` instance.

        Note, because the `MultiRLModule` held by this class is only a reference
        it is not contained in the class' state.
        """
        self._check_is_built()

        state = {
            "should_module_be_updated": self.config.policies_to_train,
            WEIGHTS_SEQ_NO: self._weights_seq_no,
        }

        if self._check_component(COMPONENT_METRICS_LOGGER, components, not_components):
            # TODO (sven): Make `MetricsLogger` a Checkpointable.
            state[COMPONENT_METRICS_LOGGER] = self.metrics.get_state()

        return state

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        """Sets the state of the `DifferentiableLearner` instance."""
        self._check_is_built()

        weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

        # Update our weights_seq_no, if the new one is > 0.
        if weights_seq_no > 0:
            self._weights_seq_no = weights_seq_no

        # Update our trainable Modules information/function via our config.
        # If not provided in state (None), all Modules will be trained by default.
        if "should_module_be_updated" in state:
            self.config.multi_agent(policies_to_train=state["should_module_be_updated"])

        # TODO (sven): Make `MetricsLogger` a Checkpointable.
        if COMPONENT_METRICS_LOGGER in state:
            self.metrics.set_state(state[COMPONENT_METRICS_LOGGER])

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args,
            {
                "config": self.config,
            },  # **kwargs
        )

    @override(Checkpointable)
    def get_checkpointable_components(self):
        if not self._check_is_built(error=False):
            self.build()
        return []

    # TODO (simon): Duplicate in Learner. Move to base class "Learnable".
    def _make_batch_if_necessary(self, training_data):
        batch = training_data.batch

        # Call the learner connector on the given `episodes` (if we have one).
        if training_data.episodes is not None:
            # If we want to learn from Episodes, we must have a LearnerConnector
            # pipeline to translate into a train batch first.
            if self._learner_connector is None:
                raise ValueError(
                    f"If episodes provided for training, Learner ({self}) must have a "
                    "LearnerConnector pipeline (but pipeline is None)!"
                )

            # Call the learner connector pipeline.
            shared_data = {}
            batch = self._learner_connector(
                rl_module=self.module,
                batch=training_data.batch if training_data.batch is not None else {},
                episodes=training_data.episodes,
                shared_data=shared_data,
                metrics=self.metrics,
            )
            # Convert to a batch.
            # TODO (sven): Try to not require MultiAgentBatch anymore.
            batch = MultiAgentBatch(
                {
                    module_id: (
                        SampleBatch(module_data, _zero_padded=True)
                        if shared_data.get(f"_zero_padded_for_mid={module_id}")
                        else SampleBatch(module_data)
                    )
                    for module_id, module_data in batch.items()
                },
                env_steps=sum(len(e) for e in training_data.episodes),
            )
        # Single-agent SampleBatch: Have to convert to MultiAgentBatch.
        elif isinstance(training_data.batch, SampleBatch):
            if len(self.module) != 1:
                raise ValueError(
                    f"SampleBatch provided, but RLModule ({self.module}) has more than "
                    f"one sub-RLModule! Need to provide MultiAgentBatch instead."
                )

            batch = MultiAgentBatch(
                {next(iter(self.module.keys())): training_data.batch},
                env_steps=len(training_data.batch),
            )
        # If we already have an `MultiAgentBatch` but with `numpy` array, convert to
        # tensors.
        elif (
            isinstance(training_data.batch, MultiAgentBatch)
            and training_data.batch.policy_batches
            and (
                isinstance(
                    next(iter(training_data.batch.policy_batches.values()))["obs"],
                    numpy.ndarray,
                )
                or next(iter(training_data.batch.policy_batches.values()))["obs"].device
                != self._device
            )
        ):
            batch = self._convert_batch_type(training_data.batch)

        return batch

    # TODO (simon): Duplicate in Learner. Move to base class "Learnable".
    def _set_slicing_by_batch_id(
        self, batch: MultiAgentBatch, *, value: bool
    ) -> MultiAgentBatch:
        """Enables slicing by batch id in the given batch.

        If the input batch contains batches of sequences we need to make sure when
        slicing happens it is sliced via batch id and not timestamp. Calling this
        method enables the same flag on each SampleBatch within the input
        MultiAgentBatch.

        Args:
            batch: The MultiAgentBatch to enable slicing by batch id on.
            value: The value to set the flag to.

        Returns:
            The input MultiAgentBatch with the indexing flag is enabled / disabled on.
        """

        for pid, policy_batch in batch.policy_batches.items():
            # We assume that arriving batches for recurrent modules OR batches that
            # have a SEQ_LENS column are already zero-padded to the max sequence length
            # and have tensors of shape [B, T, ...]. Therefore, we slice sequence
            # lengths in B. See SampleBatch for more information.
            if (
                self.module[pid].is_stateful()
                or policy_batch.get("seq_lens") is not None
            ):
                if value:
                    policy_batch.enable_slicing_by_batch_id()
                else:
                    policy_batch.disable_slicing_by_batch_id()

        return batch

    # TODO (simon): Duplicate in Learner. Move to base class "Learnable".
    def _check_is_built(self, error: bool = True) -> bool:
        if self.module is None:
            if error:
                raise ValueError(
                    "Learner.build() must be called after constructing a "
                    "Learner and before calling any methods on it."
                )
            return False
        return True

    @abc.abstractmethod
    def _get_tensor_variable(
        self,
        value: Any,
        dtype: Any = None,
        trainable: bool = False,
    ) -> TensorType:
        """Returns a framework-specific tensor variable with the initial given value.

        This is a framework specific method that should be implemented by the
        framework specific sub-classes.

        Args:
            value: The initial value for the tensor variable variable.

        Returns:
            The framework specific tensor variable of the given initial value,
            dtype and trainable/requires_grad property.
        """

    # TODO (simon): Duplicate in Learner. Move to base class "Learnable".
    def _reset(self):
        self.metrics = MetricsLogger()
        self._is_built = False

    # TODO (simon): Duplicate in Learner. Move to base class "Learnable".
    def _log_steps_trained_metrics(self, batch: MultiAgentBatch):
        """Logs this iteration's steps trained, based on given `batch`."""
        for mid, module_batch in batch.policy_batches.items():
            # Log weights seq no for this batch.
            self.metrics.log_value(
                (mid, WEIGHTS_SEQ_NO),
                self._weights_seq_no,
                window=1,
            )

            module_batch_size = len(module_batch)
            # Log average batch size (for each module).
            self.metrics.log_value(
                key=(mid, MODULE_TRAIN_BATCH_SIZE_MEAN),
                value=module_batch_size,
            )
            # Log module steps (for each module).
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_TRAINED),
                value=module_batch_size,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_TRAINED_LIFETIME),
                value=module_batch_size,
                reduce="sum",
            )
            # Log module steps (sum of all modules).
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_TRAINED),
                value=module_batch_size,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_TRAINED_LIFETIME),
                value=module_batch_size,
                reduce="sum",
            )
        # Log env steps (all modules).
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_TRAINED),
            batch.env_steps(),
            reduce="sum",
            clear_on_reduce=True,
        )
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
            batch.env_steps(),
            reduce="sum",
            with_throughput=True,
        )

    @OverrideToImplementCustomLogic
    @abc.abstractmethod
    def _make_functional_call(
        self,
        module: MultiRLModule,
        params: Dict[ModuleID, NamedParamDict],
        batch: MultiAgentBatch,
        **kwargs,
    ) -> Dict[ModuleID, Dict[str, TensorType]]:
        """Makes a functional call to a module.

        Functional calls enable the Learner to (a) use the same module as its
        `MetaLearner` and (b) to generate and apply gradients without modifying
        the `RLModule` parameters directly.

        Args:
            module: The `MultiRLModule` to be used for the functional call. Note, this
                module's `forward` method must call the `foward_train`.
            params: A dictionary containing containing for each `RLModule`'s id its
                named parameter dictionary. For functional calls to work, these
                parameters need to be cloned.
            batch: A `MultiAgentBatch` instance to be used in the functional call.

        Returns:
            A dictionary with the output of the module's forward pass.
        """

    @property
    def module(self) -> MultiRLModule:
        """The MultiRLModule reference that is being used in updates."""
        return self._module

    @module.setter
    def module(self, module: MultiRLModule) -> None:
        """Sets the `MultiRLModule`.

        Args:
            module: The reference to the `MultiRLModule` of the class that holds the
                instance of this `DifferentiableLearner` instance.
        """
        self._module = module
