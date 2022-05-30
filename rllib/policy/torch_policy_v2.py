import copy
import functools
import gym
import logging
import math
import numpy as np
import os
import threading
import time
import tree  # pip install dm_tree
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    TYPE_CHECKING,
)

import ray
from ray.util import log_once
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy import _directStepOptimizerSingleton
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import force_list, NullContextManager
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
    is_overridden,
)
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_TRAINED
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import normalize_action
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import (
    GradInfoDict,
    ModelGradients,
    ModelWeights,
    TensorType,
    TensorStructType,
    TrainerConfigDict,
)

if TYPE_CHECKING:
    from ray.rllib.evaluation import Episode  # noqa

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


@DeveloperAPI
class TorchPolicyV2(Policy):
    """PyTorch specific Policy class to use with RLlib."""

    @DeveloperAPI
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
        *,
        max_seq_len: int = 20,
    ):
        """Initializes a TorchPolicy instance.

        Args:
            observation_space: Observation space of the policy.
            action_space: Action space of the policy.
            config: The Policy's config dict.
            max_seq_len: Max sequence length for LSTM training.
        """
        self.framework = config["framework"] = "torch"

        super().__init__(observation_space, action_space, config)

        # Create model.
        model, dist_class = self._init_model_and_dist_class()

        # Create multi-GPU model towers, if necessary.
        # - The central main model will be stored under self.model, residing
        #   on self.device (normally, a CPU).
        # - Each GPU will have a copy of that model under
        #   self.model_gpu_towers, matching the devices in self.devices.
        # - Parallelization is done by splitting the train batch and passing
        #   it through the model copies in parallel, then averaging over the
        #   resulting gradients, applying these averages on the main model and
        #   updating all towers' weights from the main model.
        # - In case of just one device (1 (fake or real) GPU or 1 CPU), no
        #   parallelization will be done.

        # Get devices to build the graph on.
        worker_idx = self.config.get("worker_index", 0)
        if not config["_fake_gpus"] and ray.worker._mode() == ray.worker.LOCAL_MODE:
            num_gpus = 0
        elif worker_idx == 0:
            num_gpus = config["num_gpus"]
        else:
            num_gpus = config["num_gpus_per_worker"]
        gpu_ids = list(range(torch.cuda.device_count()))

        # Place on one or more CPU(s) when either:
        # - Fake GPU mode.
        # - num_gpus=0 (either set by user or we are in local_mode=True).
        # - No GPUs available.
        if config["_fake_gpus"] or num_gpus == 0 or not gpu_ids:
            logger.info(
                "TorchPolicy (worker={}) running on {}.".format(
                    worker_idx if worker_idx > 0 else "local",
                    "{} fake-GPUs".format(num_gpus) if config["_fake_gpus"] else "CPU",
                )
            )
            self.device = torch.device("cpu")
            self.devices = [self.device for _ in range(int(math.ceil(num_gpus)) or 1)]
            self.model_gpu_towers = [
                model if i == 0 else copy.deepcopy(model)
                for i in range(int(math.ceil(num_gpus)) or 1)
            ]
            if hasattr(self, "target_model"):
                self.target_models = {
                    m: self.target_model for m in self.model_gpu_towers
                }
            self.model = model
        # Place on one or more actual GPU(s), when:
        # - num_gpus > 0 (set by user) AND
        # - local_mode=False AND
        # - actual GPUs available AND
        # - non-fake GPU mode.
        else:
            logger.info(
                "TorchPolicy (worker={}) running on {} GPU(s).".format(
                    worker_idx if worker_idx > 0 else "local", num_gpus
                )
            )
            # We are a remote worker (WORKER_MODE=1):
            # GPUs should be assigned to us by ray.
            if ray.worker._mode() == ray.worker.WORKER_MODE:
                gpu_ids = ray.get_gpu_ids()

            if len(gpu_ids) < num_gpus:
                raise ValueError(
                    "TorchPolicy was not able to find enough GPU IDs! Found "
                    f"{gpu_ids}, but num_gpus={num_gpus}."
                )

            self.devices = [
                torch.device("cuda:{}".format(i))
                for i, id_ in enumerate(gpu_ids)
                if i < num_gpus
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

        self.dist_class = dist_class
        self.unwrapped_model = model  # used to support DistributedDataParallel

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
        self._optimizers = force_list(self.make_optimizer())

        # Backward compatibility workaround so Policy will call self.loss() directly.
        # TODO(jungong): clean up after all policies are migrated to new sub-class
        # implementation.
        self._loss = None

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

        # If set, means we are using distributed allreduce during learning.
        self.distributed_world_size = None

        self.batch_divisibility_req = self.get_batch_divisibility_req()
        self.max_seq_len = max_seq_len

    @with_lock
    @override(Policy)
    @DeveloperAPI
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
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:

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
            input_dict = SampleBatch(
                {
                    SampleBatch.OBS: obs_batch,
                    "is_training": is_training,
                }
            )
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = np.asarray(prev_action_batch)
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = np.asarray(prev_reward_batch)

        with torch.no_grad():
            explore = explore if explore is not None else self.config["explore"]
            timestep = timestep if timestep is not None else self.global_timestep
            self._is_recurrent = "state_in_0" in input_dict
            if self._is_recurrent and SampleBatch.SEQ_LENS not in input_dict:
                input_dict[SampleBatch.SEQ_LENS] = torch.ones(
                    len(input_dict[SampleBatch.OBS]), dtype=torch.int32
                )

            self._lazy_tensor_dict(input_dict)

            # Switch to eval mode.
            self.model.eval()

            # Call the exploration before_compute_actions hook.
            self.exploration.before_compute_actions(explore=explore, timestep=timestep)

            dist_inputs, state_out = self.model(input_dict)
            action_dist = self.dist_class(dist_inputs, self.model)

            # Get the exploration action from the forward results.
            actions, logp = self.exploration.get_exploration_action(
                action_distribution=action_dist, timestep=timestep, explore=explore
            )

            input_dict[SampleBatch.ACTIONS] = actions

            # Add default fetches.
            extra_fetches = {}
            # Action-dist inputs.
            if dist_inputs is not None:
                extra_fetches[SampleBatch.ACTION_DIST_INPUTS] = dist_inputs
            # Action-logp and action-prob.
            if logp is not None:
                extra_fetches[SampleBatch.ACTION_PROB] = torch.exp(logp.float())
                extra_fetches[SampleBatch.ACTION_LOGP] = logp

            return actions, state_out, extra_fetches

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def compute_log_likelihoods(
        self,
        actions: Union[List[TensorStructType], TensorStructType],
        obs_batch: Union[List[TensorStructType], TensorStructType],
        state_batches: Optional[List[TensorType]] = None,
        prev_action_batch: Optional[
            Union[List[TensorStructType], TensorStructType]
        ] = None,
        prev_reward_batch: Optional[
            Union[List[TensorStructType], TensorStructType]
        ] = None,
        actions_normalized: bool = True,
    ) -> TensorType:

        if is_overridden(self.action_sampler_fn) and not is_overridden(
            self.action_distribution_fn
        ):
            raise ValueError(
                "Cannot compute log-prob/likelihood w/o an "
                "`action_distribution_fn` and a provided "
                "`action_sampler_fn`!"
            )

        with torch.no_grad():
            input_dict = self._lazy_tensor_dict(
                {SampleBatch.CUR_OBS: obs_batch, SampleBatch.ACTIONS: actions}
            )
            if prev_action_batch is not None:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch is not None:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
            seq_lens = torch.ones(len(obs_batch), dtype=torch.int32)
            state_batches = [
                convert_to_torch_tensor(s, self.device) for s in (state_batches or [])
            ]

            # Exploration hook before each forward pass.
            self.exploration.before_compute_actions(explore=False)

            # Action dist class and inputs are generated via custom function.
            if is_overridden(self.action_distribution_fn):
                dist_inputs, dist_class, state_out = self.action_distribution_fn(
                    self.model,
                    input_dict=input_dict,
                    state_batches=state_batches,
                    seq_lens=seq_lens,
                    explore=False,
                    is_training=False,
                )
            # Default action-dist inputs calculation.
            else:
                dist_class = self.dist_class
                dist_inputs, _ = self.model(input_dict, state_batches, seq_lens)

            action_dist = dist_class(dist_inputs, self.model)

            # Normalize actions if necessary.
            actions = input_dict[SampleBatch.ACTIONS]
            if not actions_normalized and self.config["normalize_actions"]:
                actions = normalize_action(actions, self.action_space_struct)

            log_likelihoods = action_dist.logp(actions)

            return log_likelihoods

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
    def extra_grad_process(
        self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        """Called after each optimizer.zero_grad() + loss.backward() call.

        Called for each self._optimizers/loss-value pair.
        Allows for gradient processing before optimizer.step() is called.
        E.g. for gradient clipping.

        Args:
            optimizer: A torch optimizer object.
            loss: The loss tensor associated with the optimizer.

        Returns:
            An dict with information on the gradient processing step.
        """
        return {}

    @DeveloperAPI
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def extra_compute_grad_fetches(self) -> Dict[str, Any]:
        """Extra values to fetch and return from compute_gradients().

        Returns:
            Extra fetch dict to be added to the fetch dict of the
            `compute_gradients` call.
        """
        return {LEARNER_STATS_KEY: {}}  # e.g, stats, td error, etc.

    @DeveloperAPI
    @OverrideToImplementCustomLogic
    def make_optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        """Custom the local PyTorch optimizer(s) to use.

        Returns:
            The local PyTorch optimizer(s) to use for this Policy.
        """
        if hasattr(self, "config"):
            optimizers = [
                torch.optim.Adam(self.model.parameters(), lr=self.config["lr"])
            ]
        else:
            optimizers = [torch.optim.Adam(self.model.parameters())]
        if getattr(self, "exploration", None):
            optimizers = self.exploration.get_exploration_optimizer(optimizers)
        return optimizers

    def _init_model_and_dist_class(self):
        model = self.make_model()

        dist_class, _ = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"], framework=self.framework
        )

        if not (
            isinstance(dist_class, functools.partial)
            or issubclass(dist_class, TorchDistributionWrapper)
        ):
            raise ValueError(
                "`dist_class` ({}) not a TorchDistributionWrapper "
                "subclass! Make sure your `action_distribution_fn` or "
                "`make_model_and_action_dist` return a correct "
                "distribution class.".format(dist_class.__name__)
            )
        return model, dist_class

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def learn_on_batch(self, postprocessed_batch: SampleBatch) -> Dict[str, TensorType]:

        # Set Model to train mode.
        if self.model:
            self.model.train()
        # Callback handling.
        learn_stats = {}
        self.callbacks.on_learn_on_batch(
            policy=self, train_batch=postprocessed_batch, result=learn_stats
        )

        # Compute gradients (will calculate all losses and `backward()`
        # them to get the grads).
        grads, fetches = self.compute_gradients(postprocessed_batch)

        # Step the optimizers.
        self.apply_gradients(_directStepOptimizerSingleton)

        if self.model:
            fetches["model"] = self.model.metrics()
        fetches.update(
            {
                "custom_metrics": learn_stats,
                NUM_AGENT_STEPS_TRAINED: postprocessed_batch.count,
            }
        )

        return fetches

    @override(Policy)
    @DeveloperAPI
    def load_batch_into_buffer(
        self,
        batch: SampleBatch,
        buffer_index: int = 0,
    ) -> int:
        # Set the is_training flag of the batch.
        batch.set_training(True)

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
        slices = [slice.to_device(self.devices[i]) for i, slice in enumerate(slices)]
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
                "Policy.learn_on_loaded_batch()!"
            )

        # Get the correct slice of the already loaded batch to use,
        # based on offset and batch size.
        device_batch_size = self.config.get(
            "sgd_minibatch_size", self.config["train_batch_size"]
        ) // len(self.devices)

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
                batch = self._loaded_batches[0][0][offset : offset + device_batch_size]
            return self.learn_on_batch(batch)

        if len(self.devices) > 1:
            # Copy weights of main model (tower-0) to all other towers.
            state_dict = self.model.state_dict()
            # Just making sure tower-0 is really the same as self.model.
            assert self.model_gpu_towers[0] is self.model
            for tower in self.model_gpu_towers[1:]:
                tower.load_state_dict(state_dict)

        if device_batch_size >= sum(len(s) for s in self._loaded_batches[buffer_index]):
            device_batches = self._loaded_batches[buffer_index]
        else:
            device_batches = [
                b[offset : offset + device_batch_size]
                for b in self._loaded_batches[buffer_index]
            ]

        # Callback handling.
        batch_fetches = {}
        for i, batch in enumerate(device_batches):
            custom_metrics = {}
            self.callbacks.on_learn_on_batch(
                policy=self, train_batch=batch, result=custom_metrics
            )
            batch_fetches[f"tower_{i}"] = {"custom_metrics": custom_metrics}

        # Do the (maybe parallelized) gradient calculation step.
        tower_outputs = self._multi_gpu_parallel_grad_calc(device_batches)

        # Mean-reduce gradients over GPU-towers (do this on CPU: self.device).
        all_grads = []
        for i in range(len(tower_outputs[0][0])):
            if tower_outputs[0][0][i] is not None:
                all_grads.append(
                    torch.mean(
                        torch.stack([t[0][i].to(self.device) for t in tower_outputs]),
                        dim=0,
                    )
                )
            else:
                all_grads.append(None)
        # Set main model's grads to mean-reduced values.
        for i, p in enumerate(self.model.parameters()):
            p.grad = all_grads[i]

        self.apply_gradients(_directStepOptimizerSingleton)

        for i, (model, batch) in enumerate(zip(self.model_gpu_towers, device_batches)):
            batch_fetches[f"tower_{i}"].update(
                {
                    LEARNER_STATS_KEY: self.stats_fn(batch),
                    "model": model.metrics(),
                }
            )

        batch_fetches.update(self.extra_compute_grad_fetches())

        return batch_fetches

    @with_lock
    @override(Policy)
    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch: SampleBatch) -> ModelGradients:

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

        postprocessed_batch.set_training(True)
        self._lazy_tensor_dict(postprocessed_batch, device=self.devices[0])

        # Do the (maybe parallelized) gradient calculation step.
        tower_outputs = self._multi_gpu_parallel_grad_calc([postprocessed_batch])

        all_grads, grad_info = tower_outputs[0]

        grad_info["allreduce_latency"] /= len(self._optimizers)
        grad_info.update(self.stats_fn(postprocessed_batch))

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

    @DeveloperAPI
    def get_tower_stats(self, stats_name: str) -> List[TensorStructType]:
        """Returns list of per-tower stats, copied to this Policy's device.

        Args:
            stats_name: The name of the stats to average over (this str
                must exist as a key inside each tower's `tower_stats` dict).

        Returns:
            The list of stats tensor (structs) of all towers, copied to this
            Policy's device.

        Raises:
            AssertionError: If the `stats_name` cannot be found in any one
            of the tower's `tower_stats` dicts.
        """
        data = []
        for tower in self.model_gpu_towers:
            if stats_name in tower.tower_stats:
                data.append(
                    tree.map_structure(
                        lambda s: s.to(self.device), tower.tower_stats[stats_name]
                    )
                )
        assert len(data) > 0, (
            f"Stats `{stats_name}` not found in any of the towers (you have "
            f"{len(self.model_gpu_towers)} towers in total)! Make "
            "sure you call the loss function on at least one of the towers."
        )
        return data

    @override(Policy)
    @DeveloperAPI
    def get_weights(self) -> ModelWeights:
        return {k: v.cpu().detach().numpy() for k, v in self.model.state_dict().items()}

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
        return [s.detach().cpu().numpy() for s in self.model.get_initial_state()]

    @override(Policy)
    @DeveloperAPI
    def get_state(self) -> Union[Dict[str, TensorType], List[TensorType]]:
        state = super().get_state()
        state["_optimizer_variables"] = []
        for i, o in enumerate(self._optimizers):
            optim_state_dict = convert_to_numpy(o.state_dict())
            state["_optimizer_variables"].append(optim_state_dict)
        # Add exploration state.
        state["_exploration_state"] = self.exploration.get_state()
        return state

    @override(Policy)
    @DeveloperAPI
    def set_state(self, state: dict) -> None:
        # Set optimizer vars first.
        optimizer_vars = state.get("_optimizer_variables", None)
        if optimizer_vars:
            assert len(optimizer_vars) == len(self._optimizers)
            for o, s in zip(self._optimizers, optimizer_vars):
                optim_state_dict = convert_to_torch_tensor(s, device=self.device)
                o.load_state_dict(optim_state_dict)
        # Set exploration's state.
        if hasattr(self, "exploration") and "_exploration_state" in state:
            self.exploration.set_state(state=state["_exploration_state"])
        # Then the Policy's (NN) weights.
        super().set_state(state)

    @override(Policy)
    @DeveloperAPI
    def export_model(self, export_dir: str, onnx: Optional[int] = None) -> None:
        """Exports the Policy's Model to local directory for serving.

        Creates a TorchScript model and saves it.

        Args:
            export_dir: Local writable directory or filename.
            onnx: If given, will export model in ONNX format. The
                value of this parameter set the ONNX OpSet version to use.
        """
        self._lazy_tensor_dict(self._dummy_batch)
        # Provide dummy state inputs if not an RNN (torch cannot jit with
        # returned empty internal states list).
        if "state_in_0" not in self._dummy_batch:
            self._dummy_batch["state_in_0"] = self._dummy_batch[
                SampleBatch.SEQ_LENS
            ] = np.array([1.0])

        state_ins = []
        i = 0
        while "state_in_{}".format(i) in self._dummy_batch:
            state_ins.append(self._dummy_batch["state_in_{}".format(i)])
            i += 1
        dummy_inputs = {
            k: self._dummy_batch[k]
            for k in self._dummy_batch.keys()
            if k != "is_training"
        }

        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        seq_lens = self._dummy_batch[SampleBatch.SEQ_LENS]
        if onnx:
            file_name = os.path.join(export_dir, "model.onnx")
            torch.onnx.export(
                self.model,
                (dummy_inputs, state_ins, seq_lens),
                file_name,
                export_params=True,
                opset_version=onnx,
                do_constant_folding=True,
                input_names=list(dummy_inputs.keys())
                + ["state_ins", SampleBatch.SEQ_LENS],
                output_names=["output", "state_outs"],
                dynamic_axes={
                    k: {0: "batch_size"}
                    for k in list(dummy_inputs.keys())
                    + ["state_ins", SampleBatch.SEQ_LENS]
                },
            )
        else:
            traced = torch.jit.trace(self.model, (dummy_inputs, state_ins, seq_lens))
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
            functools.partial(convert_to_torch_tensor, device=device or self.device)
        )
        return postprocessed_batch

    def _multi_gpu_parallel_grad_calc(
        self, sample_batches: List[SampleBatch]
    ) -> List[Tuple[List[TensorType], GradInfoDict]]:
        """Performs a parallelized loss and gradient calculation over the batch.

        Splits up the given train batch into n shards (n=number of this
        Policy's devices) and passes each data shard (in parallel) through
        the loss function using the individual devices' models
        (self.model_gpu_towers). Then returns each tower's outputs.

        Args:
            sample_batches: A list of SampleBatch shards to
                calculate loss and gradients for.

        Returns:
            A list (one item per device) of 2-tuples, each with 1) gradient
            list and 2) grad info dict.
        """
        assert len(self.model_gpu_towers) == len(sample_batches)
        lock = threading.Lock()
        results = {}
        grad_enabled = torch.is_grad_enabled()

        def _worker(shard_idx, model, sample_batch, device):
            torch.set_grad_enabled(grad_enabled)
            try:
                with NullContextManager() if device.type == "cpu" else torch.cuda.device(  # noqa: E501
                    device
                ):
                    loss_out = force_list(
                        self.loss(model, self.dist_class, sample_batch)
                    )

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
                            if param_idx in param_indices and param.grad is not None:
                                param.grad.data.zero_()
                        # Recompute gradients of loss over all variables.
                        loss_out[opt_idx].backward(retain_graph=True)
                        grad_info.update(
                            self.extra_grad_process(opt, loss_out[opt_idx])
                        )

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
                                        g, op=torch.distributed.ReduceOp.SUM
                                    )
                            else:
                                torch.distributed.all_reduce_coalesced(
                                    grads, op=torch.distributed.ReduceOp.SUM
                                )

                            for param_group in opt.param_groups:
                                for p in param_group["params"]:
                                    if p.grad is not None:
                                        p.grad /= self.distributed_world_size

                            grad_info["allreduce_latency"] += time.time() - start

                with lock:
                    results[shard_idx] = (all_grads, grad_info)
            except Exception as e:
                import traceback

                with lock:
                    results[shard_idx] = (
                        ValueError(
                            e.args[0]
                            + "\n traceback"
                            + traceback.format_exc()
                            + "\n"
                            + "In tower {} on device {}".format(shard_idx, device)
                        ),
                        e,
                    )

        # Single device (GPU) or fake-GPU case (serialize for better
        # debugging).
        if len(self.devices) == 1 or self.config["_fake_gpus"]:
            for shard_idx, (model, sample_batch, device) in enumerate(
                zip(self.model_gpu_towers, sample_batches, self.devices)
            ):
                _worker(shard_idx, model, sample_batch, device)
                # Raise errors right away for better debugging.
                last_result = results[len(results) - 1]
                if isinstance(last_result[0], ValueError):
                    raise last_result[0] from last_result[1]
        # Multi device (GPU) case: Parallelize via threads.
        else:
            threads = [
                threading.Thread(
                    target=_worker, args=(shard_idx, model, sample_batch, device)
                )
                for shard_idx, (model, sample_batch, device) in enumerate(
                    zip(self.model_gpu_towers, sample_batches, self.devices)
                )
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
