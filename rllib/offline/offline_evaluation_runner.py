import types
from typing import TYPE_CHECKING, Any, Collection, Dict, Iterable, Optional, Union

import ray
from ray.data.iterator import DataIterator
from ray.rllib.core import (
    ALL_MODULES,
    COMPONENT_RL_MODULE,
)
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.framework import get_device, try_import_torch
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_EVALUATED,
    DATASET_NUM_ITERS_EVALUATED_LIFETIME,
    MODULE_SAMPLE_BATCH_SIZE_MEAN,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
    OFFLINE_SAMPLING_TIMER,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.minibatch_utils import MiniBatchRayDataIterator
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.runners.runner import Runner
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import DeviceType, ModuleID, StateDict, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, _ = try_import_torch()

TOTAL_EVAL_LOSS_KEY = "total_eval_loss"


class OfflineEvaluationRunner(Runner, Checkpointable):
    def __init__(
        self,
        config: "AlgorithmConfig",
        module_spec: Optional[MultiRLModuleSpec] = None,
        **kwargs,
    ):

        # This needs to be defined before we call the `Runner.__init__`
        # b/c the latter calls the `make_module` and then needs the spec.
        # TODO (simon): Check, if we make this a generic attribute.
        self.__module_spec: MultiRLModuleSpec = module_spec
        self.__dataset_iterator = None
        self.__batch_iterator = None

        Runner.__init__(self, config=config, **kwargs)
        Checkpointable.__init__(self)

        # This has to be defined after we have a `self.config`.
        self._loss_for_module_fn = types.MethodType(self.get_loss_for_module_fn(), self)

    @override(Runner)
    def run(
        self,
        explore: bool = False,
        train: bool = True,
        **kwargs,
    ) -> None:

        if self.__dataset_iterator is None:
            raise ValueError(
                f"{self} doesn't have a data iterator. Can't call `run` on "
                "`OfflineEvaluationRunner`."
            )

        if not self._batch_iterator:
            self.__batch_iterator = self._create_batch_iterator(
                **self.config.iter_batches_kwargs
            )

        # Log current weight seq no.
        self.metrics.log_value(
            key=WEIGHTS_SEQ_NO,
            value=self._weights_seq_no,
            window=1,
        )

        with self.metrics.log_time(OFFLINE_SAMPLING_TIMER):
            if explore is None:
                explore = self.config.explore

            # Evaluate on offline data.
            return self._evaluate(
                explore=explore,
                train=train,
            )

    def _create_batch_iterator(self, **kwargs) -> Iterable:

        # Return a minibatch iterator.
        return MiniBatchRayDataIterator(
            iterator=self._dataset_iterator,
            device=self._device,
            minibatch_size=self.config.offline_eval_batch_size_per_runner,
            num_iters=self.config.dataset_num_iters_per_eval_runner,
            **kwargs,
        )

    def _evaluate(
        self,
        explore: bool,
        train: bool,
    ) -> None:

        for iteration, tensor_minibatch in enumerate(self._batch_iterator):
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

            if explore:
                fwd_out = self.module.forward_exploration(
                    tensor_minibatch.policy_batches
                )
            elif train:
                fwd_out = self.module.forward_train(tensor_minibatch.policy_batches)
            else:
                fwd_out = self.module.forward_inference(tensor_minibatch.policy_batches)

            eval_loss_per_module = self.compute_eval_losses(
                fwd_out=fwd_out, batch=tensor_minibatch.policy_batches
            )

            self._log_steps_evaluated_metrics(tensor_minibatch)

        # Record the number of batches pulled from the dataset.
        self.metrics.log_value(
            # TODO (simon): Create extra eval metrics.
            (ALL_MODULES, DATASET_NUM_ITERS_EVALUATED),
            iteration + 1,
            reduce="sum",
        )
        self.metrics.log_value(
            (ALL_MODULES, DATASET_NUM_ITERS_EVALUATED_LIFETIME),
            iteration + 1,
            reduce="lifetime_sum",
        )
        # Log all individual RLModules' loss terms
        # Note: We do this only once for the last of the minibatch updates, b/c the
        # window is only 1 anyways.
        for mid, loss in convert_to_numpy(eval_loss_per_module).items():
            self.metrics.log_value(
                key=(mid, TOTAL_EVAL_LOSS_KEY),
                value=loss,
                window=1,
            )

        return self.metrics.reduce()

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (),  # *args
            {"config": self.config},  # **kwargs
        )

    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        state = {}

        if self._check_component(COMPONENT_RL_MODULE, components, not_components):
            state[COMPONENT_RL_MODULE] = self.module.get_state(
                components=self._get_subcomponents(COMPONENT_RL_MODULE, components),
                not_components=self._get_subcomponents(
                    COMPONENT_RL_MODULE, not_components
                ),
                **kwargs,
            )
            state[WEIGHTS_SEQ_NO] = self._weights_seq_no

        return state

    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""
        return convert_to_torch_tensor(struct)

    @override(Runner)
    def stop(self) -> None:
        """Releases all resources used by this EnvRunner.

        For example, when using a gym.Env in this EnvRunner, you should make sure
        that its `close()` method is called.
        """
        pass

    @override(Runner)
    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""
        pass

    @override(Runner)
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Ensures that the instances has a `MultiRLModule` and an
        environment defined.

        Raises:
            AssertionError: If the EnvRunner Actor has NOT been properly initialized.
        """
        # Make sure, we have built our RLModule properly and assigned a dataset iterator.
        assert self._dataset_iterator and hasattr(self, "module")

    @override(Runner)
    def get_metrics(self):
        return self.metrics.reduce()

    def _convert_batch_type(
        self,
        batch: MultiAgentBatch,
        to_device: bool = True,
        pin_memory: bool = False,
        use_stream: bool = False,
    ) -> MultiAgentBatch:
        batch = convert_to_torch_tensor(
            batch.policy_batches,
            device=self._device if to_device else None,
            pin_memory=pin_memory,
            use_stream=use_stream,
        )
        # TODO (sven): This computation of `env_steps` is not accurate!
        length = max(len(b) for b in batch.values())
        batch = MultiAgentBatch(batch, env_steps=length)
        return batch

    def compute_eval_losses(
        self, *, fwd_out: Dict[str, Any], batch: Dict[str, Any]
    ) -> Dict[str, Any]:

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
                )
            else:
                loss = self.compute_eval_loss_for_module(
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                )
            loss_per_module[module_id] = loss

        return loss_per_module

    def compute_eval_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        return self._loss_for_module_fn(
            module_id=module_id,
            config=config,
            batch=batch,
            fwd_out=fwd_out,
        )

    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:

        # Update the RLModule state.
        if COMPONENT_RL_MODULE in state:
            # A missing value for WEIGHTS_SEQ_NO or a value of 0 means: Force the
            # update.
            weights_seq_no = state.get(WEIGHTS_SEQ_NO, 0)

            # Only update the weigths, if this is the first synchronization or
            # if the weights of this `EnvRunner` lacks behind the actual ones.
            if weights_seq_no == 0 or self._weights_seq_no < weights_seq_no:
                rl_module_state = state[COMPONENT_RL_MODULE]
                if isinstance(rl_module_state, ray.ObjectRef):
                    rl_module_state = ray.get(rl_module_state)
                self.module.set_state(rl_module_state)

            # Update our weights_seq_no, if the new one is > 0.
            if weights_seq_no > 0:
                self._weights_seq_no = weights_seq_no

    def _log_steps_evaluated_metrics(self, batch: MultiAgentBatch) -> None:
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
                key=(mid, MODULE_SAMPLE_BATCH_SIZE_MEAN),
                value=module_batch_size,
            )
            # Log module steps (for each module).
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_SAMPLED),
                value=module_batch_size,
                reduce="sum",
            )
            self.metrics.log_value(
                key=(mid, NUM_MODULE_STEPS_SAMPLED_LIFETIME),
                value=module_batch_size,
                reduce="lifetime_sum",
            )
            # Log module steps (sum of all modules).
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_SAMPLED),
                value=module_batch_size,
                reduce="sum",
            )
            self.metrics.log_value(
                key=(ALL_MODULES, NUM_MODULE_STEPS_SAMPLED_LIFETIME),
                value=module_batch_size,
                reduce="lifetime_sum",
            )
        # Log env steps (all modules).
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_SAMPLED),
            batch.env_steps(),
            reduce="sum",
        )
        self.metrics.log_value(
            (ALL_MODULES, NUM_ENV_STEPS_SAMPLED_LIFETIME),
            batch.env_steps(),
            reduce="lifetime_sum",
            with_throughput=True,
        )

    @override(Runner)
    def set_device(self):
        try:
            self.__device = get_device(
                self.config,
                (
                    0
                    if not self.worker_index
                    else self.config.num_gpus_per_offline_eval_runner
                ),
            )
        except NotImplementedError:
            self.__device = None

    @override(Runner)
    def make_module(self):
        try:
            from ray.rllib.env import INPUT_ENV_SPACES

            if not self._module_spec:
                self.__module_spec = self.config.get_multi_rl_module_spec(
                    # Note, usually we have no environemnt in case of offline evaluation.
                    env=self.config.env,
                    spaces={
                        INPUT_ENV_SPACES: (
                            self.config.observation_space,
                            self.config.action_space,
                        )
                    },
                    inference_only=self.config.offline_eval_rl_module_inference_only,
                )
            # Build the module from its spec.
            self.module = self._module_spec.build()
            # TODO (simon): Implement GPU inference.
            # Move the RLModule to our device.
            # TODO (sven): In order to make this framework-agnostic, we should maybe
            #  make the MultiRLModule.build() method accept a device OR create an
            #  additional `(Multi)RLModule.to()` override.

            self.module.foreach_module(
                lambda mid, mod: (
                    mod.to(self._device) if isinstance(mod, torch.nn.Module) else mod
                )
            )

        # If `AlgorithmConfig.get_multi_rl_module_spec()` is not implemented, this env runner
        # will not have an RLModule, but might still be usable with random actions.
        except NotImplementedError:
            self.module = None

    def get_loss_for_module_fn(self):
        # Either the user has provided a loss-for-module function, or we take
        # the loss function from the default `Learner` class.
        return (
            self.config.offline_loss_for_module_fn
            or self.config.get_default_learner_class().__dict__[
                "compute_loss_for_module"
            ]
        )

    @property
    def _dataset_iterator(self) -> DataIterator:
        """Returns the dataset iterator."""
        return self.__dataset_iterator

    def set_dataset_iterator(self, iterator):
        """Sets the dataset iterator."""
        self.__dataset_iterator = iterator

    @property
    def _batch_iterator(self) -> MiniBatchRayDataIterator:
        return self.__batch_iterator

    @property
    def _device(self) -> Union[DeviceType, None]:
        return self.__device

    @property
    def _module_spec(self) -> MultiRLModuleSpec:
        """Returns the `MultiRLModuleSpec` of this `Runner`."""
        return self.__module_spec
