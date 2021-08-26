import copy
import functools
import gym
import logging
import math
import numpy as np
import os
import time
import threading
from typing import Callable, Dict, List, Optional, Set, Tuple, Type, Union, \
    TYPE_CHECKING

import ray
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.utils import force_list, NullContextManager
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.schedules import PiecewiseSchedule
from ray.rllib.utils.spaces.space_utils import normalize_action
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.torch_ops import convert_to_non_torch_type, \
    convert_to_torch_tensor
from ray.rllib.utils.typing import ModelGradients, ModelWeights, TensorType, \
    TrainerConfigDict

if TYPE_CHECKING:
    from ray.rllib.evaluation import MultiAgentEpisode  # noqa

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


@DeveloperAPI
class TorchPolicy(Policy):
    """Template for a PyTorch policy and loss to use with RLlib.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        config (dict): config of the policy.
        model (TorchModel): Torch model instance.
        dist_class (type): Torch action distribution class.
    """

    @DeveloperAPI
    def __init__(
            self,
            observation_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            config: TrainerConfigDict,
            *,
            model: ModelV2,
            loss: Callable[[
                Policy, ModelV2, Type[TorchDistributionWrapper], SampleBatch
            ], Union[TensorType, List[TensorType]]],
            action_distribution_class: Type[TorchDistributionWrapper],
            action_sampler_fn: Optional[Callable[[
                TensorType, List[TensorType]
            ], Tuple[TensorType, TensorType]]] = None,
            action_distribution_fn: Optional[Callable[[
                Policy, ModelV2, TensorType, TensorType, TensorType
            ], Tuple[TensorType, Type[TorchDistributionWrapper], List[
                TensorType]]]] = None,
            max_seq_len: int = 20,
            get_batch_divisibility_req: Optional[Callable[[Policy],
                                                          int]] = None,
    ):
        """Build a policy from policy and loss torch modules.

        Note that model will be placed on GPU device if CUDA_VISIBLE_DEVICES
        is set. Only single GPU is supported for now.

        Args:
            observation_space (gym.spaces.Space): observation space of the
                policy.
            action_space (gym.spaces.Space): action space of the policy.
            config (TrainerConfigDict): The Policy config dict.
            model (ModelV2): PyTorch policy module. Given observations as
                input, this module must return a list of outputs where the
                first item is action logits, and the rest can be any value.
            loss (Callable[[Policy, ModelV2, Type[TorchDistributionWrapper],
                SampleBatch], Union[TensorType, List[TensorType]]]): Callable
                that returns a single scalar loss or a list of loss terms.
            action_distribution_class (Type[TorchDistributionWrapper]): Class
                for a torch action distribution.
            action_sampler_fn (Callable[[TensorType, List[TensorType]],
                Tuple[TensorType, TensorType]]): A callable returning a
                sampled action and its log-likelihood given Policy, ModelV2,
                input_dict, explore, timestep, and is_training.
            action_distribution_fn (Optional[Callable[[Policy, ModelV2,
                ModelInputDict, TensorType, TensorType],
                Tuple[TensorType, type, List[TensorType]]]]): A callable
                returning distribution inputs (parameters), a dist-class to
                generate an action distribution object from, and
                internal-state outputs (or an empty list if not applicable).
                Note: No Exploration hooks have to be called from within
                `action_distribution_fn`. It's should only perform a simple
                forward pass through some model.
                If None, pass inputs through `self.model()` to get distribution
                inputs.
                The callable takes as inputs: Policy, ModelV2, ModelInputDict,
                explore, timestep, is_training.
            max_seq_len (int): Max sequence length for LSTM training.
            get_batch_divisibility_req (Optional[Callable[[Policy], int]]]):
                Optional callable that returns the divisibility requirement
                for sample batches given the Policy.
        """
        self.framework = "torch"
        super().__init__(observation_space, action_space, config)

        # Create multi-GPU model towers, if necessary.
        # - The central main model will be stored under self.model, residing
        #   on self.device.
        # - Each GPU will have a copy of that model under
        #   self.model_gpu_towers, matching the devices in self.devices.
        # - Parallelization is done by splitting the train batch and passing
        #   it through the model copies in parallel, then averaging over the
        #   resulting gradients, applying these averages on the main model and
        #   updating all towers' weights from the main model.
        # - In case of just one device (1 (fake) GPU or 1 CPU), no
        #   parallelization will be done.

        # Get devices to build the graph on.
        worker_idx = self.config.get("worker_index", 0)
        if not config["_fake_gpus"] and \
                ray.worker._mode() == ray.worker.LOCAL_MODE:
            num_gpus = 0
        elif worker_idx == 0:
            num_gpus = config["num_gpus"]
        else:
            num_gpus = config["num_gpus_per_worker"]
        gpu_ids = list(range(torch.cuda.device_count()))

        # Place on one or more CPU(s) when either:
        # - Fake GPU mode.
        # - num_gpus=0 (either set by user or we are in local_mode=True).
        # - no GPUs available.
        if config["_fake_gpus"] or num_gpus == 0 or not gpu_ids:
            logger.info("TorchPolicy (worker={}) running on {}.".format(
                worker_idx
                if worker_idx > 0 else "local", "{} fake-GPUs".format(num_gpus)
                if config["_fake_gpus"] else "CPU"))
            self.device = torch.device("cpu")
            self.devices = [
                self.device for _ in range(int(math.ceil(num_gpus)) or 1)
            ]
            self.model_gpu_towers = [
                model if i == 0 else copy.deepcopy(model)
                for i in range(int(math.ceil(num_gpus)) or 1)
            ]
            if hasattr(self, "target_model"):
                self.target_models = {
                    m: self.target_model
                    for m in self.model_gpu_towers
                }
            self.model = model
        # Place on one or more actual GPU(s), when:
        # - num_gpus > 0 (set by user) AND
        # - local_mode=False AND
        # - actual GPUs available AND
        # - non-fake GPU mode.
        else:
            logger.info("TorchPolicy (worker={}) running on {} GPU(s).".format(
                worker_idx if worker_idx > 0 else "local", num_gpus))
            # We are a remote worker (WORKER_MODE=1):
            # GPUs should be assigned to us by ray.
            if ray.worker._mode() == ray.worker.WORKER_MODE:
                gpu_ids = ray.get_gpu_ids()

            if len(gpu_ids) < num_gpus:
                raise ValueError(
                    "TorchPolicy was not able to find enough GPU IDs! Found "
                    f"{gpu_ids}, but num_gpus={num_gpus}.")

            self.devices = [
                torch.device("cuda:{}".format(i))
                for i, id_ in enumerate(gpu_ids) if i < num_gpus
            ]
            self.device = self.devices[0]
            ids = [id_ for i, id_ in enumerate(gpu_ids) if i < num_gpus]
            self.model_gpu_towers = []
            for i, _ in enumerate(ids):
                model_copy = copy.deepcopy(model)
                self.model_gpu_towers.append(model_copy.to(self.devices[i]))
            if hasattr(self, "target_model"):
                self.target_models = {
                    m: copy.deepcopy(self.target_model).to(self.devices[i])
                    for i, m in enumerate(self.model_gpu_towers)
                }
            self.model = self.model_gpu_towers[0]

        # Lock used for locking some methods on the object-level.
        # This prevents possible race conditions when calling the model
        # first, then its value function (e.g. in a loss function), in
        # between of which another model call is made (e.g. to compute an
        # action).
        self._lock = threading.RLock()

        self._state_inputs = self.model.get_initial_state()
        self._is_recurrent = len(self._state_inputs) > 0
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()
        # Combine view_requirements for Model and Policy.
        self.view_requirements.update(self.model.view_requirements)

        self.exploration = self._create_exploration()
        self.unwrapped_model = model  # used to support DistributedDataParallel
        self._loss = loss
        self._optimizers = force_list(self.optimizer())
        # Store, which params (by index within the model's list of
        # parameters) should be updated per optimizer.
        # Maps optimizer idx to set or param indices.
        self.multi_gpu_param_groups: List[Set[int]] = []
        main_params = {p: i for i, p in enumerate(self.model.parameters())}
        for o in self._optimizers:
            param_indices = []
            for pg_idx, pg in enumerate(o.param_groups):
                for p in pg["params"]:
                    param_indices.append(main_params[p])
            self.multi_gpu_param_groups.append(set(param_indices))

        # Create n sample-batch buffers (num_multi_gpu_tower_stacks), each
        # one with m towers (num_gpus).
        num_buffers = self.config.get("num_multi_gpu_tower_stacks", 1)
        self._loaded_batches = [[] for _ in range(num_buffers)]

        self.dist_class = action_distribution_class
        self.action_sampler_fn = action_sampler_fn
        self.action_distribution_fn = action_distribution_fn

        # If set, means we are using distributed allreduce during learning.
        self.distributed_world_size = None

        self.max_seq_len = max_seq_len
        self.batch_divisibility_req = get_batch_divisibility_req(self) if \
            callable(get_batch_divisibility_req) else \
            (get_batch_divisibility_req or 1)

    @override(Policy)
    @DeveloperAPI
    def compute_actions(
            self,
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Union[List[TensorType], TensorType] = None,
            prev_reward_batch: Union[List[TensorType], TensorType] = None,
            info_batch: Optional[Dict[str, list]] = None,
            episodes: Optional[List["MultiAgentEpisode"]] = None,
            explore: Optional[bool] = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        with torch.no_grad():
            seq_lens = torch.ones(len(obs_batch), dtype=torch.int32)
            input_dict = self._lazy_tensor_dict(
                SampleBatch({
                    SampleBatch.CUR_OBS: np.asarray(obs_batch),
                }))
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = \
                    np.asarray(prev_action_batch)
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = \
                    np.asarray(prev_reward_batch)
            state_batches = [
                convert_to_torch_tensor(s, self.device)
                for s in (state_batches or [])
            ]
            return self._compute_action_helper(input_dict, state_batches,
                                               seq_lens, explore, timestep)

    @override(Policy)
    def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        with torch.no_grad():
            # Pass lazy (torch) tensor dict to Model as `input_dict`.
            input_dict = self._lazy_tensor_dict(input_dict)
            # Pack internal state inputs into (separate) list.
            state_batches = [
                input_dict[k] for k in input_dict.keys() if "state_in" in k[:8]
            ]
            # Calculate RNN sequence lengths.
            seq_lens = np.array([1] * len(input_dict["obs"])) \
                if state_batches else None

            return self._compute_action_helper(input_dict, state_batches,
                                               seq_lens, explore, timestep)

    @with_lock
    def _compute_action_helper(self, input_dict, state_batches, seq_lens,
                               explore, timestep):
        """Shared forward pass logic (w/ and w/o trajectory view API).

        Returns:
            Tuple:
                - actions, state_out, extra_fetches, logp.
        """
        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep
        self._is_recurrent = state_batches is not None and state_batches != []

        # Switch to eval mode.
        if self.model:
            self.model.eval()

        if self.action_sampler_fn:
            action_dist = dist_inputs = None
            actions, logp, state_out = self.action_sampler_fn(
                self,
                self.model,
                input_dict,
                state_batches,
                explore=explore,
                timestep=timestep)
        else:
            # Call the exploration before_compute_actions hook.
            self.exploration.before_compute_actions(
                explore=explore, timestep=timestep)
            if self.action_distribution_fn:
                # Try new action_distribution_fn signature, supporting
                # state_batches and seq_lens.
                try:
                    dist_inputs, dist_class, state_out = \
                        self.action_distribution_fn(
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
                        dist_inputs, dist_class, state_out = \
                            self.action_distribution_fn(
                                self,
                                self.model,
                                input_dict[SampleBatch.CUR_OBS],
                                explore=explore,
                                timestep=timestep,
                                is_training=False)
                    else:
                        raise e
            else:
                dist_class = self.dist_class
                dist_inputs, state_out = self.model(input_dict, state_batches,
                                                    seq_lens)

            if not (isinstance(dist_class, functools.partial)
                    or issubclass(dist_class, TorchDistributionWrapper)):
                raise ValueError(
                    "`dist_class` ({}) not a TorchDistributionWrapper "
                    "subclass! Make sure your `action_distribution_fn` or "
                    "`make_model_and_action_dist` return a correct "
                    "distribution class.".format(dist_class.__name__))
            action_dist = dist_class(dist_inputs, self.model)

            # Get the exploration action from the forward results.
            actions, logp = \
                self.exploration.get_exploration_action(
                    action_distribution=action_dist,
                    timestep=timestep,
                    explore=explore)

        input_dict[SampleBatch.ACTIONS] = actions

        # Add default and custom fetches.
        extra_fetches = self.extra_action_out(input_dict, state_batches,
                                              self.model, action_dist)

        # Action-dist inputs.
        if dist_inputs is not None:
            extra_fetches[SampleBatch.ACTION_DIST_INPUTS] = dist_inputs

        # Action-logp and action-prob.
        if logp is not None:
            extra_fetches[SampleBatch.ACTION_PROB] = \
                torch.exp(logp.float())
            extra_fetches[SampleBatch.ACTION_LOGP] = logp

        # Update our global timestep by the batch size.
        self.global_timestep += len(input_dict[SampleBatch.CUR_OBS])

        return convert_to_non_torch_type((actions, state_out, extra_fetches))

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def compute_log_likelihoods(
            self,
            actions: Union[List[TensorType], TensorType],
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            prev_reward_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            actions_normalized: bool = True,
    ) -> TensorType:

        if self.action_sampler_fn and self.action_distribution_fn is None:
            raise ValueError("Cannot compute log-prob/likelihood w/o an "
                             "`action_distribution_fn` and a provided "
                             "`action_sampler_fn`!")

        with torch.no_grad():
            input_dict = self._lazy_tensor_dict({
                SampleBatch.CUR_OBS: obs_batch,
                SampleBatch.ACTIONS: actions
            })
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
            seq_lens = torch.ones(len(obs_batch), dtype=torch.int32)
            state_batches = [
                convert_to_torch_tensor(s, self.device)
                for s in (state_batches or [])
            ]

            # Exploration hook before each forward pass.
            self.exploration.before_compute_actions(explore=False)

            # Action dist class and inputs are generated via custom function.
            if self.action_distribution_fn:

                # Try new action_distribution_fn signature, supporting
                # state_batches and seq_lens.
                try:
                    dist_inputs, dist_class, state_out = \
                        self.action_distribution_fn(
                            self,
                            self.model,
                            input_dict=input_dict,
                            state_batches=state_batches,
                            seq_lens=seq_lens,
                            explore=False,
                            is_training=False)
                # Trying the old way (to stay backward compatible).
                # TODO: Remove in future.
                except TypeError as e:
                    if "positional argument" in e.args[0] or \
                            "unexpected keyword argument" in e.args[0]:
                        dist_inputs, dist_class, _ = \
                            self.action_distribution_fn(
                                policy=self,
                                model=self.model,
                                obs_batch=input_dict[SampleBatch.CUR_OBS],
                                explore=False,
                                is_training=False)
                    else:
                        raise e

            # Default action-dist inputs calculation.
            else:
                dist_class = self.dist_class
                dist_inputs, _ = self.model(input_dict, state_batches,
                                            seq_lens)

            action_dist = dist_class(dist_inputs, self.model)

            # Normalize actions if necessary.
            actions = input_dict[SampleBatch.ACTIONS]
            if not actions_normalized and self.config["normalize_actions"]:
                actions = normalize_action(actions, self.action_space_struct)

            log_likelihoods = action_dist.logp(actions)

            return log_likelihoods

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def learn_on_batch(
            self, postprocessed_batch: SampleBatch) -> Dict[str, TensorType]:

        # Set Model to train mode.
        if self.model:
            self.model.train()
        # Callback handling.
        learn_stats = {}
        self.callbacks.on_learn_on_batch(
            policy=self, train_batch=postprocessed_batch, result=learn_stats)

        # Compute gradients (will calculate all losses and `backward()`
        # them to get the grads).
        grads, fetches = self.compute_gradients(postprocessed_batch)

        # Step the optimizers.
        self.apply_gradients(_directStepOptimizerSingleton)

        if self.model:
            fetches["model"] = self.model.metrics()
        fetches.update({"custom_metrics": learn_stats})

        return fetches

    @override(Policy)
    @DeveloperAPI
    def load_batch_into_buffer(
            self,
            batch: SampleBatch,
            buffer_index: int = 0,
    ) -> int:
        # Set the is_training flag of the batch.
        batch.is_training = True

        # Shortcut for 1 CPU only: Store batch in `self._loaded_batches`.
        if len(self.devices) == 1 and self.devices[0].type == "cpu":
            assert buffer_index == 0
            pad_batch_to_sequences_of_same_size(
                batch=batch,
                max_seq_len=self.max_seq_len,
                shuffle=False,
                batch_divisibility_req=self.batch_divisibility_req,
                view_requirements=self.view_requirements,
            )
            self._lazy_tensor_dict(batch)
            self._loaded_batches[0] = [batch]
            return len(batch)

        # Batch (len=28, seq-lens=[4, 7, 4, 10, 3]):
        # 0123 0123456 0123 0123456789ABC

        # 1) split into n per-GPU sub batches (n=2).
        # [0123 0123456] [012] [3 0123456789 ABC]
        # (len=14, 14 seq-lens=[4, 7, 3] [1, 10, 3])
        slices = batch.timeslices(num_slices=len(self.devices))

        # 2) zero-padding (max-seq-len=10).
        # - [0123000000 0123456000 0120000000]
        # - [3000000000 0123456789 ABC0000000]
        for slice in slices:
            pad_batch_to_sequences_of_same_size(
                batch=slice,
                max_seq_len=self.max_seq_len,
                shuffle=False,
                batch_divisibility_req=self.batch_divisibility_req,
                view_requirements=self.view_requirements,
            )

        # 3) Load splits into the given buffer (consisting of n GPUs).
        slices = [
            slice.to_device(self.devices[i]) for i, slice in enumerate(slices)
        ]
        self._loaded_batches[buffer_index] = slices

        # Return loaded samples per-device.
        return len(slices[0])

    @override(Policy)
    @DeveloperAPI
    def get_num_samples_loaded_into_buffer(self, buffer_index: int = 0) -> int:
        if len(self.devices) == 1 and self.devices[0] == "/cpu:0":
            assert buffer_index == 0
        return len(self._loaded_batches[buffer_index])

    @override(Policy)
    @DeveloperAPI
    def learn_on_loaded_batch(self, offset: int = 0, buffer_index: int = 0):
        if not self._loaded_batches[buffer_index]:
            raise ValueError(
                "Must call Policy.load_batch_into_buffer() before "
                "Policy.learn_on_loaded_batch()!")

        # Get the correct slice of the already loaded batch to use,
        # based on offset and batch size.
        device_batch_size = \
            self.config.get(
                "sgd_minibatch_size", self.config["train_batch_size"]) // \
            len(self.devices)

        # Set Model to train mode.
        if self.model_gpu_towers:
            for t in self.model_gpu_towers:
                t.train()

        # Shortcut for 1 CPU only: Batch should already be stored in
        # `self._loaded_batches`.
        if len(self.devices) == 1 and self.devices[0].type == "cpu":
            assert buffer_index == 0
            if device_batch_size >= len(self._loaded_batches[0][0]):
                batch = self._loaded_batches[0][0]
            else:
                batch = self._loaded_batches[0][0][offset:offset +
                                                   device_batch_size]
            return self.learn_on_batch(batch)

        if len(self.devices) > 1:
            # Copy weights of main model (tower-0) to all other towers.
            state_dict = self.model.state_dict()
            # Just making sure tower-0 is really the same as self.model.
            assert self.model_gpu_towers[0] is self.model
            for tower in self.model_gpu_towers[1:]:
                tower.load_state_dict(state_dict)

        if device_batch_size >= sum(
                len(s) for s in self._loaded_batches[buffer_index]):
            device_batches = self._loaded_batches[buffer_index]
        else:
            device_batches = [
                b[offset:offset + device_batch_size]
                for b in self._loaded_batches[buffer_index]
            ]

        # Do the (maybe parallelized) gradient calculation step.
        tower_outputs = self._multi_gpu_parallel_grad_calc(device_batches)

        # Mean-reduce gradients over GPU-towers (do this on CPU: self.device).
        all_grads = []
        for i in range(len(tower_outputs[0][0])):
            if tower_outputs[0][0][i] is not None:
                all_grads.append(
                    torch.mean(
                        torch.stack(
                            [t[0][i].to(self.device) for t in tower_outputs]),
                        dim=0))
            else:
                all_grads.append(None)
        # Set main model's grads to mean-reduced values.
        for i, p in enumerate(self.model.parameters()):
            p.grad = all_grads[i]

        self.apply_gradients(_directStepOptimizerSingleton)

        batch_fetches = {}
        for i, batch in enumerate(device_batches):
            batch_fetches[f"tower_{i}"] = {
                LEARNER_STATS_KEY: self.extra_grad_info(batch)
            }

        batch_fetches.update(self.extra_compute_grad_fetches())

        return batch_fetches

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def compute_gradients(self,
                          postprocessed_batch: SampleBatch) -> ModelGradients:

        assert len(self.devices) == 1

        # If not done yet, see whether we have to zero-pad this batch.
        if not postprocessed_batch.zero_padded:
            pad_batch_to_sequences_of_same_size(
                batch=postprocessed_batch,
                max_seq_len=self.max_seq_len,
                shuffle=False,
                batch_divisibility_req=self.batch_divisibility_req,
                view_requirements=self.view_requirements,
            )

        postprocessed_batch.is_training = True
        self._lazy_tensor_dict(postprocessed_batch, device=self.devices[0])

        # Do the (maybe parallelized) gradient calculation step.
        tower_outputs = self._multi_gpu_parallel_grad_calc(
            [postprocessed_batch])

        all_grads, grad_info = tower_outputs[0]

        grad_info["allreduce_latency"] /= len(self._optimizers)
        grad_info.update(self.extra_grad_info(postprocessed_batch))

        fetches = self.extra_compute_grad_fetches()

        return all_grads, dict(fetches, **{LEARNER_STATS_KEY: grad_info})

    @override(Policy)
    @DeveloperAPI
    def apply_gradients(self, gradients: ModelGradients) -> None:
        if gradients == _directStepOptimizerSingleton:
            for i, opt in enumerate(self._optimizers):
                opt.step()
        else:
            # TODO(sven): Not supported for multiple optimizers yet.
            assert len(self._optimizers) == 1
            for g, p in zip(gradients, self.model.parameters()):
                if g is not None:
                    if torch.is_tensor(g):
                        p.grad = g.to(self.device)
                    else:
                        p.grad = torch.from_numpy(g).to(self.device)

            self._optimizers[0].step()

    @override(Policy)
    @DeveloperAPI
    def get_weights(self) -> ModelWeights:
        return {
            k: v.cpu().detach().numpy()
            for k, v in self.model.state_dict().items()
        }

    @override(Policy)
    @DeveloperAPI
    def set_weights(self, weights: ModelWeights) -> None:
        weights = convert_to_torch_tensor(weights, device=self.device)
        self.model.load_state_dict(weights)

    @override(Policy)
    @DeveloperAPI
    def is_recurrent(self) -> bool:
        return self._is_recurrent

    @override(Policy)
    @DeveloperAPI
    def num_state_tensors(self) -> int:
        return len(self.model.get_initial_state())

    @override(Policy)
    @DeveloperAPI
    def get_initial_state(self) -> List[TensorType]:
        return [
            s.detach().cpu().numpy() for s in self.model.get_initial_state()
        ]

    @override(Policy)
    @DeveloperAPI
    def get_state(self) -> Union[Dict[str, TensorType], List[TensorType]]:
        state = super().get_state()
        state["_optimizer_variables"] = []
        for i, o in enumerate(self._optimizers):
            optim_state_dict = convert_to_non_torch_type(o.state_dict())
            state["_optimizer_variables"].append(optim_state_dict)
        # Add exploration state.
        state["_exploration_state"] = \
            self.exploration.get_state()
        return state

    @override(Policy)
    @DeveloperAPI
    def set_state(self, state: dict) -> None:
        # Set optimizer vars first.
        optimizer_vars = state.get("_optimizer_variables", None)
        if optimizer_vars:
            assert len(optimizer_vars) == len(self._optimizers)
            for o, s in zip(self._optimizers, optimizer_vars):
                optim_state_dict = convert_to_torch_tensor(
                    s, device=self.device)
                o.load_state_dict(optim_state_dict)
        # Set exploration's state.
        if hasattr(self, "exploration") and "_exploration_state" in state:
            self.exploration.set_state(state=state["_exploration_state"])
        # Then the Policy's (NN) weights.
        super().set_state(state)

    @DeveloperAPI
    def extra_grad_process(self, optimizer: "torch.optim.Optimizer",
                           loss: TensorType):
        """Called after each optimizer.zero_grad() + loss.backward() call.

        Called for each self._optimizers/loss-value pair.
        Allows for gradient processing before optimizer.step() is called.
        E.g. for gradient clipping.

        Args:
            optimizer (torch.optim.Optimizer): A torch optimizer object.
            loss (TensorType): The loss tensor associated with the optimizer.

        Returns:
            Dict[str, TensorType]: An dict with information on the gradient
                processing step.
        """
        return {}

    @DeveloperAPI
    def extra_compute_grad_fetches(self) -> Dict[str, any]:
        """Extra values to fetch and return from compute_gradients().

        Returns:
            Dict[str, any]: Extra fetch dict to be added to the fetch dict
                of the compute_gradients call.
        """
        return {LEARNER_STATS_KEY: {}}  # e.g, stats, td error, etc.

    @DeveloperAPI
    def extra_action_out(
            self, input_dict: Dict[str, TensorType],
            state_batches: List[TensorType], model: TorchModelV2,
            action_dist: TorchDistributionWrapper) -> Dict[str, TensorType]:
        """Returns dict of extra info to include in experience batch.

        Args:
            input_dict (Dict[str, TensorType]): Dict of model input tensors.
            state_batches (List[TensorType]): List of state tensors.
            model (TorchModelV2): Reference to the model object.
            action_dist (TorchDistributionWrapper): Torch action dist object
                to get log-probs (e.g. for already sampled actions).

        Returns:
            Dict[str, TensorType]: Extra outputs to return in a
                compute_actions() call (3rd return value).
        """
        return {}

    @DeveloperAPI
    def extra_grad_info(self,
                        train_batch: SampleBatch) -> Dict[str, TensorType]:
        """Return dict of extra grad info.

        Args:
            train_batch (SampleBatch): The training batch for which to produce
                extra grad info for.

        Returns:
            Dict[str, TensorType]: The info dict carrying grad info per str
                key.
        """
        return {}

    @DeveloperAPI
    def optimizer(
            self
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        """Custom the local PyTorch optimizer(s) to use.

        Returns:
            Union[List[torch.optim.Optimizer], torch.optim.Optimizer]:
                The local PyTorch optimizer(s) to use for this Policy.
        """
        if hasattr(self, "config"):
            return torch.optim.Adam(
                self.model.parameters(), lr=self.config["lr"])
        else:
            return torch.optim.Adam(self.model.parameters())

    @override(Policy)
    @DeveloperAPI
    def export_model(self, export_dir: str,
                     onnx: Optional[int] = None) -> None:
        """Exports the Policy's Model to local directory for serving.

        Creates a TorchScript model and saves it.

        Args:
            export_dir (str): Local writable directory or filename.
        """
        self._lazy_tensor_dict(self._dummy_batch)
        # Provide dummy state inputs if not an RNN (torch cannot jit with
        # returned empty internal states list).
        if "state_in_0" not in self._dummy_batch:
            self._dummy_batch["state_in_0"] = \
                self._dummy_batch[SampleBatch.SEQ_LENS] = np.array([1.0])

        state_ins = []
        i = 0
        while "state_in_{}".format(i) in self._dummy_batch:
            state_ins.append(self._dummy_batch["state_in_{}".format(i)])
            i += 1
        dummy_inputs = {
            k: self._dummy_batch[k]
            for k in self._dummy_batch.keys() if k != "is_training"
        }

        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        seq_lens = self._dummy_batch[SampleBatch.SEQ_LENS]
        if onnx:
            file_name = os.path.join(export_dir, "model.onnx")
            torch.onnx.export(
                self.model, (dummy_inputs, state_ins, seq_lens),
                file_name,
                export_params=True,
                opset_version=onnx,
                do_constant_folding=True,
                input_names=list(dummy_inputs.keys()) +
                ["state_ins", SampleBatch.SEQ_LENS],
                output_names=["output", "state_outs"],
                dynamic_axes={
                    k: {
                        0: "batch_size"
                    }
                    for k in list(dummy_inputs.keys()) +
                    ["state_ins", SampleBatch.SEQ_LENS]
                })
        else:
            traced = torch.jit.trace(self.model,
                                     (dummy_inputs, state_ins, seq_lens))
            file_name = os.path.join(export_dir, "model.pt")
            traced.save(file_name)

    @override(Policy)
    def export_checkpoint(self, export_dir: str) -> None:
        raise NotImplementedError

    @override(Policy)
    @DeveloperAPI
    def import_model_from_h5(self, import_file: str) -> None:
        """Imports weights into torch model."""
        return self.model.import_from_h5(import_file)

    def _lazy_tensor_dict(self, postprocessed_batch: SampleBatch, device=None):
        # TODO: (sven): Keep for a while to ensure backward compatibility.
        if not isinstance(postprocessed_batch, SampleBatch):
            postprocessed_batch = SampleBatch(postprocessed_batch)
        postprocessed_batch.set_get_interceptor(
            functools.partial(
                convert_to_torch_tensor, device=device or self.device))
        return postprocessed_batch

    def _multi_gpu_parallel_grad_calc(self, sample_batches):
        """Performs a parallelized loss and gradient calculation over the batch.

        Splits up the given train batch into n shards (n=number of this
        Policy's devices) and passes each data shard (in parallel) through
        the loss function using the individual devices' models
        (self.model_gpu_towers). Then returns each tower's outputs.

        Args:
            sample_batches (List[SampleBatch]): A list of SampleBatch shards to
                calculate loss and gradients for.

        Returns:
            List[Tuple[List[TensorType], StatsDict]]: A list (one item per
                device) of 2-tuples with 1) gradient list and 2) stats dict.
        """
        assert len(self.model_gpu_towers) == len(sample_batches)
        lock = threading.Lock()
        results = {}
        grad_enabled = torch.is_grad_enabled()

        def _worker(shard_idx, model, sample_batch, device):
            torch.set_grad_enabled(grad_enabled)
            try:
                with NullContextManager(
                ) if device.type == "cpu" else torch.cuda.device(device):
                    loss_out = force_list(
                        self._loss(self, model, self.dist_class, sample_batch))

                    # Call Model's custom-loss with Policy loss outputs and
                    # train_batch.
                    loss_out = model.custom_loss(loss_out, sample_batch)

                    assert len(loss_out) == len(self._optimizers)

                    # Loop through all optimizers.
                    grad_info = {"allreduce_latency": 0.0}

                    parameters = list(model.parameters())
                    all_grads = [None for _ in range(len(parameters))]
                    for opt_idx, opt in enumerate(self._optimizers):
                        # Erase gradients in all vars of the tower that this
                        # optimizer would affect.
                        param_indices = self.multi_gpu_param_groups[opt_idx]
                        for param_idx, param in enumerate(parameters):
                            if param_idx in param_indices and \
                                    param.grad is not None:
                                param.grad.data.zero_()
                        # Recompute gradients of loss over all variables.
                        loss_out[opt_idx].backward(retain_graph=True)
                        grad_info.update(
                            self.extra_grad_process(opt, loss_out[opt_idx]))

                        grads = []
                        # Note that return values are just references;
                        # Calling zero_grad would modify the values.
                        for param_idx, param in enumerate(parameters):
                            if param_idx in param_indices:
                                if param.grad is not None:
                                    grads.append(param.grad)
                                all_grads[param_idx] = param.grad

                        if self.distributed_world_size:
                            start = time.time()
                            if torch.cuda.is_available():
                                # Sadly, allreduce_coalesced does not work with
                                # CUDA yet.
                                for g in grads:
                                    torch.distributed.all_reduce(
                                        g, op=torch.distributed.ReduceOp.SUM)
                            else:
                                torch.distributed.all_reduce_coalesced(
                                    grads, op=torch.distributed.ReduceOp.SUM)

                            for param_group in opt.param_groups:
                                for p in param_group["params"]:
                                    if p.grad is not None:
                                        p.grad /= self.distributed_world_size

                            grad_info[
                                "allreduce_latency"] += time.time() - start

                with lock:
                    results[shard_idx] = (all_grads, grad_info)
            except Exception as e:
                with lock:
                    results[shard_idx] = (ValueError(
                        e.args[0] + "\n" +
                        "In tower {} on device {}".format(shard_idx, device)),
                                          e)

        # Single device (GPU) or fake-GPU case (serialize for better
        # debugging).
        if len(self.devices) == 1 or self.config["_fake_gpus"]:
            for shard_idx, (model, sample_batch, device) in enumerate(
                    zip(self.model_gpu_towers, sample_batches, self.devices)):
                _worker(shard_idx, model, sample_batch, device)
                # Raise errors right away for better debugging.
                last_result = results[len(results) - 1]
                if isinstance(last_result[0], ValueError):
                    raise last_result[0] from last_result[1]
        # Multi device (GPU) case: Parallelize via threads.
        else:
            threads = [
                threading.Thread(
                    target=_worker,
                    args=(shard_idx, model, sample_batch, device))
                for shard_idx, (model, sample_batch, device) in enumerate(
                    zip(self.model_gpu_towers, sample_batches, self.devices))
            ]

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        # Gather all threads' outputs and return.
        outputs = []
        for shard_idx in range(len(sample_batches)):
            output = results[shard_idx]
            if isinstance(output[0], Exception):
                raise output[0] from output[1]
            outputs.append(results[shard_idx])
        return outputs


# TODO: (sven) Unify hyperparam annealing procedures across RLlib (tf/torch)
#   and for all possible hyperparams, not just lr.
@DeveloperAPI
class LearningRateSchedule:
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self._lr_schedule = None
        if lr_schedule is None:
            self.cur_lr = lr
        else:
            self._lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None)
            self.cur_lr = self._lr_schedule.value(0)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        if self._lr_schedule:
            self.cur_lr = self._lr_schedule.value(global_vars["timestep"])
            for opt in self._optimizers:
                for p in opt.param_groups:
                    p["lr"] = self.cur_lr


@DeveloperAPI
class EntropyCoeffSchedule:
    """Mixin for TorchPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self._entropy_coeff_schedule = None
        if entropy_coeff_schedule is None:
            self.entropy_coeff = entropy_coeff
        else:
            # Allows for custom schedule similar to lr_schedule format
            if isinstance(entropy_coeff_schedule, list):
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    entropy_coeff_schedule,
                    outside_value=entropy_coeff_schedule[-1][-1],
                    framework=None)
            else:
                # Implements previous version but enforces outside_value
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    [[0, entropy_coeff], [entropy_coeff_schedule, 0.0]],
                    outside_value=0.0,
                    framework=None)
            self.entropy_coeff = self._entropy_coeff_schedule.value(0)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        if self._entropy_coeff_schedule is not None:
            self.entropy_coeff = self._entropy_coeff_schedule.value(
                global_vars["timestep"])


@DeveloperAPI
class DirectStepOptimizer:
    """Typesafe method for indicating apply gradients can directly step the
       optimizers with in-place gradients.
    """
    _instance = None

    def __new__(cls):
        if DirectStepOptimizer._instance is None:
            DirectStepOptimizer._instance = super().__new__(cls)
        return DirectStepOptimizer._instance

    def __eq__(self, other):
        return type(self) == type(other)

    def __repr__(self):
        return "DirectStepOptimizer"


_directStepOptimizerSingleton = DirectStepOptimizer()
