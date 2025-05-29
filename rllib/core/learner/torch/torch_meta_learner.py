import contextlib
import logging
import ray

from itertools import cycle
from typing import Any, Dict, List, Optional, Tuple

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.torch.torch_differentiable_learner import (
    TorchDifferentiableLearner,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_TRAINED,
    DATASET_NUM_ITERS_TRAINED_LIFETIME,
    DIFFERENTIABLE_LEARNER_RESULTS,
)
from ray.rllib.utils.metrics.utils import to_snake_case
from ray.rllib.utils.typing import (
    EpisodeType,
    ModuleID,
    NamedParamDict,
    ResultDict,
    TensorType,
)

logger = logging.getLogger("__name__")

torch, nn = try_import_torch()


class TorchMetaLearner(TorchLearner):
    """A `TorchLearner` designed for meta-learning with functional updates.

    This `TorchLearner` manages one or more `DifferentiableLearner` instances,
    which perform functional updates on the `MultiRLModule`. These updates enable
    the computation of higher-order gradients, making this class suitable for
    meta-learning applications.

    The `update` method executes one or more update loops on its
    `DifferentiableLearner` instances, leveraging the functionally updated
    parameters for its own learning process.
    """

    # A list of `TorchDifferentiableLearner`s that run inner functional update
    # loops.
    others: List[TorchDifferentiableLearner]

    def __init__(
        self,
        **kwargs,
    ):
        # First, initialize the `TorchLearner`.
        super().__init__(**kwargs)

        # Initialize all configured `TorchDifferentiableLearner`s.
        self.others = [
            other_config.learner_class(
                config=self.config,
                learner_config=other_config,
                module=self.module,
            )
            for other_config in self.config.differentiable_learner_configs
        ]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(TorchLearner)
    def build(self) -> None:
        """Builds the `TorchMetaLearner`.

        This method should be called before the meta-learner is used. It is
        responsible for setting up the `TorchDifferentiableLearner`s.
        """
        super().build()

        # Build all `DifferentiableLearner`s.
        for other in self.others:
            other.build(device=self._device)

        # Ensure 'others' have a module reference.
        for other in self.others:
            if not other.module:
                other.module = self.module

    @OverrideToImplementCustomLogic
    @override(Learner)
    def update(
        self,
        batch: Optional[MultiAgentBatch] = None,
        batches: Optional[List[MultiAgentBatch]] = None,
        batch_refs: Optional[List[ray.ObjectRef]] = None,
        episodes: Optional[List[EpisodeType]] = None,
        episodes_refs: Optional[List[ray.ObjectRef]] = None,
        data_iterators: Optional[List[ray.data.DataIterator]] = None,
        training_data: Optional[TrainingData] = None,
        *,
        # TODO (simon): Maybe defined here other_training_data to provide
        # TrainingData for differentiable Learners. List to nest them.
        # other_training_data: List[Union[List[Any], TrainingData]]
        # TODO (sven): Make this a more formal structure with its own type.
        timesteps: Optional[Dict[str, Any]] = None,
        num_total_minibatches: int = 0,
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        _no_metrics_reduce: bool = False,
        others_training_data: Optional[List[TrainingData]] = None,
        **kwargs,
    ) -> ResultDict:
        """Performs a meta-update on the `MultiRLModule`.

        This method allows multiple backward passes on a batch by iteratively
        applying functional updates from the `TorchDifferentiableLearner` instances
        in `others`. The resulting differentiable parameters are then used in this
        learner's own functional forward pass on the `MultiRLModule`, enabling the
        computation of higher-order gradients during the backward pass.
        """
        self._check_is_built()

        # Call `before_gradient_based_update` to allow for non-gradient based
        # preparations-, logging-, and update logic to happen.
        self.before_gradient_based_update(timesteps=timesteps or {})

        if training_data is None:
            training_data = TrainingData(
                batch=batch,
                batches=batches,
                batch_refs=batch_refs,
                episodes=episodes,
                episodes_refs=episodes_refs,
                data_iterators=data_iterators,
            )
        # Validate training data.
        # TODO (simon): Pass in a `TrainingData` list for all differentiable learners.
        # to be used if necessary. Some algorithms need it.
        training_data.validate()
        training_data.solve_refs()
        assert training_data.batches is None, "`training_data.batches` must be None!"

        # Increase the _weigths_seq_no in each `update` run.
        self._weights_seq_no += 1

        # Create a batch iterator.
        # TODO (simon): Create this method in the `Learner`.
        batch_iter = self._create_iterator_if_necessary(
            training_data=training_data,
            num_total_minibatches=num_total_minibatches,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch,
            **kwargs,
        )

        # If no training data for `DifferentiableLearner`s have been passed in, cycle
        # over the main training_data for each `DifferentiableLearner`.
        if not others_training_data:
            others_training_data = cycle([training_data])

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

            # Clone the parameters for the differentiable updates.
            params = self._clone_named_parameters()

            # Update all differentiable learners.
            others_loss_per_module = []
            others_results = {}
            for other, other_training_data in zip(self.others, others_training_data):
                # TODO (simon): Maybe return all losses from each other's update step.
                params, other_loss_per_module, other_results = other.update(
                    training_data=other_training_data,
                    params=params,
                    # TODO (simon): Check, if this is still needed.
                    _no_metrics_reduce=_no_metrics_reduce,
                    **kwargs,
                )
                others_loss_per_module.append(other_loss_per_module)
                # TODO (simon): Find a more elegant way for naming.
                others_results[to_snake_case(other.__class__.__name__)] = other_results

            # Log training results from the `DifferentiableLearner`s.
            # TODO (simon): Right now metrics are not carried over b/c of
            #   the double tensormode problem.
            self.metrics.aggregate(
                stats_dicts=[others_results], key=DIFFERENTIABLE_LEARNER_RESULTS
            )

            # Make the actual in-graph/traced meta-`_update` call. This should return
            # all tensor values (no numpy).
            fwd_out, loss_per_module, _ = self._update(
                tensor_minibatch.policy_batches,
                params,
                others_loss_per_module,
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
        self.after_gradient_based_update(timesteps=timesteps or {})

        self.metrics.deactivate_tensor_mode()

        # Reduce results across all minibatch update steps.
        if not _no_metrics_reduce:
            return self.metrics.reduce()

    def _clone_named_parameters(self):
        """Clone named parameters for functional updates."""
        return self.module.foreach_module(
            lambda _, m: {name: p.clone() for name, p in m.named_parameters()},
            return_dict=True,
        )

    @OverrideToImplementCustomLogic
    @override(TorchLearner)
    def _update(
        self,
        batch: Dict[str, Any],
        params: NamedParamDict,
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
    ) -> Tuple[Any, Any, Any]:
        # The first time we call _update after building the learner or
        # adding/removing models, we update with the uncompiled update method.
        # This makes it so that any variables that may be created during the first
        # update step are already there when compiling. More specifically,
        # this avoids errors that occur around using defaultdicts with
        # torch.compile().
        if (
            self._torch_compile_complete_update
            and not self._compiled_update_initialized
        ):
            self._compiled_update_initialized = True
            return self._uncompiled_update(batch, params, others_loss_per_module)
        else:
            return self._possibly_compiled_update(batch, params, others_loss_per_module)

    def _uncompiled_update(
        self,
        batch: Dict,
        params: NamedParamDict,
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
        **kwargs,
    ):
        """Performs a single functional update using a batch of data.

        This update utilizes a functional forward pass via PyTorch 2.0's `func` module.
        The updated parameters provided by `TorchDifferentiableLearner` instances are
        functions of the `MultiRLModule`'s parameters, allowing for differentiation.

        To ensure compatibility, the `MultiRLModule`'s `forward` method must encapsulate
        all logic from `forward_train` and support additional keyword arguments (`kwargs`).
        """
        # TODO (sven): Causes weird cuda error when WandB is used.
        #  Diagnosis thus far:
        #  - All peek values during metrics.reduce are non-tensors.
        #  - However, in impala.py::training_step(), a tensor does arrive after learner
        #    group.update(), so somehow, there is still a race condition
        #    possible (learner, which performs the reduce() and learner thread, which
        #    performs the logging of tensors into metrics logger).
        self._compute_off_policyness(batch)

        # TODO (simon): For this to work, the `RLModule.forward` must run the
        # the `forward_train`. Passing in arguments which makes the `forward` modular
        # could be a workaround.
        # Make a functional forward call to include higher-order gradients in the meta
        # update.
        fwd_out = self._make_functional_call(params, batch)
        loss_per_module = self.compute_losses(
            fwd_out=fwd_out, batch=batch, others_loss_per_module=others_loss_per_module
        )
        gradients = self.compute_gradients(loss_per_module)

        with contextlib.ExitStack() as stack:
            if self.config.num_learners > 1:
                for mod in self.module.values():
                    # Skip non-torch modules, b/c they may not have the `no_sync` API.
                    if isinstance(mod, torch.nn.Module):
                        stack.enter_context(mod.no_sync())
            postprocessed_gradients = self.postprocess_gradients(gradients)
            self.apply_gradients(postprocessed_gradients)

        # Deactivate tensor-mode on our MetricsLogger and collect the (tensor)
        # results.
        return fwd_out, loss_per_module, {}

    @override(Learner)
    def compute_losses(
        self,
        *,
        fwd_out: Dict[str, Any],
        batch: Dict[str, Any],
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
        **kwargs,
    ) -> Dict[ModuleID, TensorType]:
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

        Losses from `DifferentiableLearner` instances in `others_loss_per_module`
        can be leveraged for more advanced module-wise loss calculations.

        Args:
            fwd_out: Output from a call to the `forward_train()` method of the
                underlying MultiRLModule (`self.module`) during training
                (`self.update()`).
            batch: The train batch that was used to compute `fwd_out`.
            others_loss_per_module: A list of losses per module id from the contained
                `DifferentiableLearner` instances in`self.others`.

        Returns:
            A dictionary mapping module IDs to individual loss terms.
        """
        loss_per_module = {}
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            module = self.module[module_id].unwrapped()
            if isinstance(module, SelfSupervisedLossAPI):
                loss = module.compute_self_supervised_loss(
                    learner=self,
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                    others_loss_per_module=others_loss_per_module,
                )
            else:
                loss = self.compute_loss_for_module(
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                    others_loss_per_module=others_loss_per_module,
                )
            loss_per_module[module_id] = loss

        return loss_per_module

    @OverrideToImplementCustomLogic
    @override(Learner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
    ) -> TensorType:
        """Computes the loss for a single module.

        This method calculates the loss for an individual agent. In multi-agent
        scenarios requiring more complex loss computations, consider overriding the
        `compute_losses` method instead.

        Losses from `DifferentiableLearner` instances in `others_loss_per_module`
        can be leveraged for more advanced module-wise loss calculations.

        Args:
            module_id: The id of the module.
            config: The AlgorithmConfig specific to the given `module_id`.
            batch: The train batch for this particular module.
            fwd_out: The output of the forward pass for this particular module.
            others_loss_per_module: A list of losses per module id from the contained
                `DifferentiableLearner` instances in`self.others`.

        Returns:
            A single total loss tensor. If you have more than one optimizer on the
            provided `module_id` and would like to compute gradients separately using
            these different optimizers, simply add up the individual loss terms for
            each optimizer and return the sum. Also, for recording/logging any
            individual loss terms, you can use the `Learner.metrics.log_value(
            key=..., value=...)` or `Learner.metrics.log_dict()` APIs. See:
            :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger` for more
            information.
        """
        # By default, `others_loss_per_module` is not used; instead, the superclass
        # method is called.
        return super().compute_loss_for_module(
            module_id=module_id, config=config, batch=batch, fwd_out=fwd_out
        )

    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:
        """Make a functional forward call to all modules in the `MultiRLModule`."""
        return self._module.foreach_module(
            lambda mid, m: torch.func.functional_call(m, params[mid], batch[mid]),
            return_dict=True,
        )
