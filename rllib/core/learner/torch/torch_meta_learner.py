import contextlib
import logging
import ray

from typing import Any, Dict, List, Optional, Tuple
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.learner.torch.torch_differentiable_learner import (
    TorchDifferentiableLearner,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.learner.training_data import TrainingData
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
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import (
    EpisodeType,
    ModuleID,
    NamedParamDict,
    ParamDict,
    ResultDict,
    TensorType,
)

logger = logging.getLogger("__name__")

torch, nn = try_import_torch()


class TorchMetaLearner(TorchLearner):

    others: List[TorchDifferentiableLearner]

    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.others = [
            other_config.learner_class(
                config=self.config,
                learner_config=other_config,
                module=self.module,
                # TODO (simon): Create this attribute in the `AlgorithmConfig`
                # or generate a `DifferentiableAlgorithmConfig` (naming is weird)
                # that has this attribute (and maybe others).
            )
            for other_config in self.config.differentiable_learner_configs
        ]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(TorchLearner)
    def build(self) -> None:
        """Builds the Learner.

        This method should be called before the learner is used. It is responsible for
        setting up the LearnerConnectorPipeline, the RLModule, optimizer(s), and
        (optionally) the optimizers' learning rate schedulers.
        """
        super().build()

        # Build all `DifferentiableLearner`s.
        for other in self.others:
            other.build()

        # Ensure 'others' have a module reference.
        for other in self.others:
            if not other.module:
                other.module = self.module

    @OverrideToImplementCustomLogic
    @override(TorchLearner)
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
        **kwargs,
    ) -> ResultDict:

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
        training_data.validate()
        training_data.solve_refs()
        assert training_data.batches is None, "`training_data.batches` must be None!"

        self._weights_seq_no += 1

        batch_iter = self._create_iterator_if_necessary(
            training_data=training_data,
            num_total_minibatches=num_total_minibatches,
            num_epochs=num_epochs,
            minibatch_size=minibatch_size,
            shuffle_batch_per_epoch=shuffle_batch_per_epoch,
            **kwargs,
        )

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

            params = self._clone_named_parameters()

            # Update all differentiable learners.
            other_losses = []
            for other in self.others:
                # TODO (simon): Add the single results to the overall results.
                params, other_results = other.update(
                    training_data=training_data,
                    params=params,
                    _no_metrics_reduce=_no_metrics_reduce,
                    **kwargs,
                )
                other_losses.append(other_results)
            for mid, module_params in params.items():
                for name, p in module_params.items():
                    print(f"{name}: requires_grad = {p.requires_grad}")
            # for mid, module_params in params.items():
            #     for name, p in module_params.items():
            #         if p.grad is not None:
            #             print(f"{name}: {p.grad.norm()}")
            #         else:
            #             print(f"{name}: No gradient")

            # Make the actual in-graph/traced meta-`_update` call. This should return
            # all tensor values (no numpy).
            fwd_out, loss_per_module, tensor_metrics = self._update(
                tensor_minibatch.policy_batches,
                params,
            )

            # Convert logged tensor metrics (logged during tensor-mode of MetricsLogger)
            # to actual (numpy) values.
            self.metrics.tensors_to_numpy(tensor_metrics)

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
        for mid, loss in convert_to_numpy(loss_per_module).items():
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

        # Reduce results across all minibatch update steps.
        if not _no_metrics_reduce:
            return self.metrics.reduce()

    def _clone_named_parameters(self):

        return self.module.foreach_module(
            lambda _, m: {name: p.clone() for name, p in m.named_parameters()},
            return_dict=True,
        )

    @OverrideToImplementCustomLogic
    @override(TorchLearner)
    def _update(
        self, batch: Dict[str, Any], params: NamedParamDict
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
            return self._uncompiled_update(batch, params)
        else:
            return self._possibly_compiled_update(batch, params)

    def _uncompiled_update(
        self,
        batch: Dict,
        params: NamedParamDict,
        **kwargs,
    ):
        """Performs a single update given a batch of data."""
        # Activate tensor-mode on our MetricsLogger.
        self.metrics.activate_tensor_mode()

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
        fwd_out = self._make_functional_call(params, batch)
        loss_per_module = self.compute_losses(fwd_out=fwd_out, batch=batch)
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
        return fwd_out, loss_per_module, self.metrics.deactivate_tensor_mode()

    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:

        return self._module.foreach_module(
            lambda mid, m: torch.func.functional_call(m, params[mid], batch[mid]),
            return_dict=True,
        )

    @override(TorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:
        for optim in self._optimizer_parameters:
            # `set_to_none=True` is a faster way to zero out the gradients.
            optim.zero_grad(set_to_none=True)

        if self._grad_scalers is not None:
            total_loss = sum(
                self._grad_scalers[mid].scale(loss)
                for mid, loss in loss_per_module.items()
            )
        else:
            total_loss = sum(loss_per_module.values())

        # If we don't have any loss computations, `sum` returns 0.
        if isinstance(total_loss, int):
            assert total_loss == 0
            return {}

        total_loss.backward(retain_graph=True)
        grads = {pid: p.grad for pid, p in self._params.items()}

        return grads
