import logging
import numpy
import ray
from typing import Any, Dict, List, Iterable, Optional, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import ALL_MODULES
from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils import unflatten_dict
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_TRAINED,
    DATASET_NUM_ITERS_TRAINED_LIFETIME,
)
from ray.rllib.utils.minibatch_utils import (
    MiniBatchCyclicIterator,
    MiniBatchDummyIterator,
    MiniBatchRayDataIterator,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import EpisodeType, ResultDict

logger = logging.getLogger(__name__)


class MetaLearner(Learner):

    others: List[DifferentiableLearner]

    def __init__(
        self,
        *,
        config: "AlgorithmConfig",
        module_spec: Optional[Union[RLModuleSpec, MultiRLModuleSpec]] = None,
        module: Optional[RLModule] = None,
    ):
        super().__init__(
            config=config,
            module_spec=module_spec,
            module=module,
        )

        # Initialize all `DifferentiableLearner` instances.

        self.others = [
            other_config.get_default_learner_class()(
                config=other_config,
                module=self.module,
            )
            for other_config in self.config.differentiable_learner_configs
        ]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def build(self) -> None:
        """Builds the Learner.

        This method should be called before the learner is used. It is responsible for
        setting up the LearnerConnectorPipeline, the RLModule, optimizer(s), and
        (optionally) the optimizers' learning rate schedulers.
        """
        super().__init__()

        if self._is_built:
            logger.debug("Learner already built. Skipping build.")
            return

        # Build all `DifferentiableLearner`s.
        for other in self.others:
            other.build()

        # Ensure 'others' have a module reference.
        for other in self.others:
            if not other.module:
                other.set_module(self.module)

    def _get_named_parameters(self):

        return self.module.foreach_module(
            lambda _, m: {name: p for name, p in m.named_parameters()}, return_dict=True
        )

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
        # TODO (sven): Make this a more formal structure with its own type.
        timesteps: Optional[Dict[str, Any]] = None,
        num_total_minibatches: int = 0,
        num_epochs: int = 1,
        minibatch_size: Optional[int] = None,
        shuffle_batch_per_epoch: bool = False,
        _no_metrics_reduce: bool = False,
        **kwargs,
    ) -> ResultDict:
        """Run `num_epochs` epochs over the given train batch.

        You can use this method to take more than one backward pass on the batch.
        The same `minibatch_size` and `num_epochs` will be used for all module ids in
        MultiRLModule.

        Args:
            batch: A batch of training data to update from.
            timesteps: Timesteps dict, which must have the key
                `NUM_ENV_STEPS_SAMPLED_LIFETIME`.
                # TODO (sven): Make this a more formal structure with its own type.
            num_epochs: The number of complete passes over the entire train batch. Each
                pass might be further split into n minibatches (if `minibatch_size`
                provided).
            minibatch_size: The size of minibatches to use to further split the train
                `batch` into sub-batches. The `batch` is then iterated over n times
                where n is `len(batch) // minibatch_size`.
            shuffle_batch_per_epoch: Whether to shuffle the train batch once per epoch.
                If the train batch has a time rank (axis=1), shuffling will only take
                place along the batch axis to not disturb any intact (episode)
                trajectories. Also, shuffling is always skipped if `minibatch_size` is
                None, meaning the entire train batch is processed each epoch, making it
                unnecessary to shuffle.

        Returns:
            A `ResultDict` object produced by a call to `self.metrics.reduce()`. The
            returned dict may be arbitrarily nested and must have `Stats` objects at
            all its leafs, allowing components further downstream (i.e. a user of this
            Learner) to further reduce these results (for example over n parallel
            Learners).
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

            # Use cloned parameters for the inner loop.
            params = self._clone_named_parameters()

            # Run the update of the `DifferentiableLearner`.
            for other in self.others:
                # TODO (simon): Solve this differently, by either providing other's
                # data in a list `List[TrainingData]` or creating it inside the inner loop.
                # TODO (simon): Maybe providing here also the module.
                params, other_metrics = other.update(
                    batch=batch,
                    params=params,
                    batches=batches,
                    batch_refs=batch_refs,
                    episodes=episodes,
                    episodes_refs=episodes_refs,
                    data_iterators=data_iterators,
                    timesteps=timesteps,
                    num_total_minibatches=num_total_minibatches,
                    num_epochs=num_epochs,
                    minibatch_size=minibatch_size,
                    shuffle_batch_per_epoch=shuffle_batch_per_epoch,
                    _no_metrics_reduce=_no_metrics_reduce,
                    **kwargs,
                )

            # Make the actual in-graph/traced `_update` call. This should return
            # all tensor values (no numpy).
            fwd_out, loss_per_module, tensor_metrics = self._update(
                tensor_minibatch.policy_batches
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

    def _clone_named_parameters(self):

        return self.module.foreach_module(
            lambda _, m: {name: p.clone() for name, p in m.named_parameters()},
            return_dict=True,
        )
