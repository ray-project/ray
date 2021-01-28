import functools
import gym
import numpy as np
import logging
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.jax.jax_modelv2 import JAXModelV2
from ray.rllib.models.jax.jax_action_dist import JAXDistribution
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.utils.jax_ops import convert_to_jax_device_array, \
    convert_to_non_jax_type
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule
from ray.rllib.utils.tracking_dict import UsageTrackingDict
from ray.rllib.utils.typing import ModelGradients, ModelWeights, \
    TensorType, TrainerConfigDict

jax, flax = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp

logger = logging.getLogger(__name__)


@DeveloperAPI
class JAXPolicy(Policy):
    """Template for a JAX policy and loss to use with RLlib.
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
                Policy, ModelV2, Type[JAXDistribution], SampleBatch
            ], Union[TensorType, List[TensorType]]],
            action_distribution_class: Type[JAXDistribution],
            action_sampler_fn: Optional[Callable[[
                TensorType, List[TensorType]
            ], Tuple[TensorType, TensorType]]] = None,
            action_distribution_fn: Optional[Callable[[
                Policy, ModelV2, TensorType, TensorType, TensorType
            ], Tuple[TensorType, Type[JAXDistribution], List[
                TensorType]]]] = None,
            max_seq_len: int = 20,
            get_batch_divisibility_req: Optional[Callable[[Policy],
                                                          int]] = None,
    ):
        """Initializes a JAXPolicy instance.

        Args:
            observation_space (gym.spaces.Space): Observation space of the
                policy.
            action_space (gym.spaces.Space): Action space of the policy.
            config (TrainerConfigDict): The Policy config dict.
            model (ModelV2): PyTorch policy module. Given observations as
                input, this module must return a list of outputs where the
                first item is action logits, and the rest can be any value.
            loss (Callable[[Policy, ModelV2, Type[JAXDistribution],
                SampleBatch], Union[TensorType, List[TensorType]]]): Callable
                that returns a single scalar loss or a list of loss terms.
            action_distribution_class (Type[JAXDistribution]): Class
                for a torch action distribution.
            action_sampler_fn (Callable[[TensorType, List[TensorType]],
                Tuple[TensorType, TensorType]]): A callable returning a
                sampled action and its log-likelihood given Policy, ModelV2,
                input_dict, explore, timestep, and is_training.
            action_distribution_fn (Optional[Callable[[Policy, ModelV2,
                Dict[str, TensorType], TensorType, TensorType],
                Tuple[TensorType, type, List[TensorType]]]]): A callable
                returning distribution inputs (parameters), a dist-class to
                generate an action distribution object from, and
                internal-state outputs (or an empty list if not applicable).
                Note: No Exploration hooks have to be called from within
                `action_distribution_fn`. It's should only perform a simple
                forward pass through some model.
                If None, pass inputs through `self.model()` to get distribution
                inputs.
                The callable takes as inputs: Policy, ModelV2, input_dict,
                explore, timestep, is_training.
            max_seq_len (int): Max sequence length for LSTM training.
            get_batch_divisibility_req (Optional[Callable[[Policy], int]]]):
                Optional callable that returns the divisibility requirement
                for sample batches given the Policy.
        """
        self.framework = "jax"
        super().__init__(observation_space, action_space, config)
        self.model = model
        # Auto-update model's inference view requirements, if recurrent.
        self._update_model_view_requirements_from_init_state()
        # Combine view_requirements for Model and Policy.
        self.view_requirements.update(self.model.view_requirements)

        self.exploration = self._create_exploration()
        self.unwrapped_model = model  # used to support DistributedDataParallel
        self._loss = loss
        #self._gradient_loss = jax.grad(self._loss, argnums=4)
        self._optimizers = force_list(self.optimizer())

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

        input_dict = self._lazy_tensor_dict({
            SampleBatch.CUR_OBS: np.asarray(obs_batch),
            "is_training": False,
        })
        if prev_action_batch is not None:
            input_dict[SampleBatch.PREV_ACTIONS] = \
                np.asarray(prev_action_batch)
        if prev_reward_batch is not None:
            input_dict[SampleBatch.PREV_REWARDS] = \
                np.asarray(prev_reward_batch)
        for i, s in enumerate(state_batches or []):
            input_dict["state_in_".format(i)] = s

        return self.compute_actions_from_input_dict(input_dict, explore,
                                                    timestep, **kwargs)

    @override(Policy)
    def compute_actions_from_input_dict(
            self,
            input_dict: Dict[str, TensorType],
            explore: bool = None,
            timestep: Optional[int] = None,
            **kwargs) -> \
            Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        import time#TODO
        start = time.time()

        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep

        # Pack internal state inputs into (separate) list.
        state_batches = [
            input_dict[k] for k in input_dict.keys() if "state_in" in k[:8]
        ]
        # Calculate RNN sequence lengths.
        seq_lens = np.array([1] * len(input_dict["obs"])) \
            if state_batches else None

        #if self.action_sampler_fn:
        #    action_dist = dist_inputs = None
        #    state_out = state_batches
        #    actions, logp, state_out = self.action_sampler_fn(
        #        self,
        #        self.model,
        #        input_dict,
        #        state_out,
        #        explore=explore,
        #        timestep=timestep)
        #else:
        # Call the exploration before_compute_actions hook.
        #self.exploration.before_compute_actions(
        #    explore=explore, timestep=timestep)
        #if self.action_distribution_fn:
        #    dist_inputs, dist_class, state_out = \
        #        self.action_distribution_fn(
        #            self,
        #            self.model,
        #            input_dict[SampleBatch.CUR_OBS],
        #            explore=explore,
        #            timestep=timestep,
        #            is_training=False)
        #else:
        dist_class = self.dist_class
        dist_inputs, value_out, state_out = self.model(
            input_dict, state_batches, seq_lens)

        #if not (isinstance(dist_class, functools.partial)
        #        or issubclass(dist_class, JAXDistribution)):
        #    raise ValueError(
        #        "`dist_class` ({}) not a JAXDistribution "
        #        "subclass! Make sure your `action_distribution_fn` or "
        #        "`make_model_and_action_dist` return a correct "
        #        "distribution class.".format(dist_class.__name__))
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
        extra_fetches[SampleBatch.VF_PREDS] = value_out

        # Action-dist inputs.
        if dist_inputs is not None:
            extra_fetches[SampleBatch.ACTION_DIST_INPUTS] = dist_inputs

        # Action-logp and action-prob.
        if logp is not None:
            extra_fetches[SampleBatch.ACTION_PROB] = \
                jnp.exp(logp.astype(jnp.float32))
            extra_fetches[SampleBatch.ACTION_LOGP] = logp

        # Update our global timestep by the batch size.
        self.global_timestep += len(input_dict[SampleBatch.CUR_OBS])

        #TODO
        print("action pass={}".format(time.time() - start))

        return convert_to_non_jax_type((actions, state_out, extra_fetches))

    @override(Policy)
    @DeveloperAPI
    def compute_log_likelihoods(
            self,
            actions: Union[List[TensorType], TensorType],
            obs_batch: Union[List[TensorType], TensorType],
            state_batches: Optional[List[TensorType]] = None,
            prev_action_batch: Optional[Union[List[TensorType],
                                              TensorType]] = None,
            prev_reward_batch: Optional[Union[List[
                TensorType], TensorType]] = None) -> TensorType:

        if self.action_sampler_fn and self.action_distribution_fn is None:
            raise ValueError("Cannot compute log-prob/likelihood w/o an "
                             "`action_distribution_fn` and a provided "
                             "`action_sampler_fn`!")

        input_dict = {
            SampleBatch.CUR_OBS: obs_batch,
            SampleBatch.ACTIONS: actions
        }
        if prev_action_batch is not None:
            input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
        if prev_reward_batch is not None:
            input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
        seq_lens = jnp.ones(len(obs_batch), dtype=jnp.int32)
        state_batches = [s for s in (state_batches or [])]

        # Exploration hook before each forward pass.
        self.exploration.before_compute_actions(explore=False)

        # Action dist class and inputs are generated via custom function.
        if self.action_distribution_fn:
            dist_inputs, dist_class, _ = self.action_distribution_fn(
                policy=self,
                model=self.model,
                obs_batch=input_dict[SampleBatch.CUR_OBS],
                explore=False,
                is_training=False)
        # Default action-dist inputs calculation.
        else:
            dist_class = self.dist_class
            dist_inputs, _ = self.model(input_dict, state_batches, seq_lens)

        action_dist = dist_class(dist_inputs, self.model)
        log_likelihoods = action_dist.logp(input_dict[SampleBatch.ACTIONS])
        return log_likelihoods

    @override(Policy)
    @DeveloperAPI
    def learn_on_batch(
            self, postprocessed_batch: SampleBatch) -> Dict[str, TensorType]:
        # Callback handling.
        self.callbacks.on_learn_on_batch(
            policy=self, train_batch=postprocessed_batch)

        # Compute gradients (will calculate all losses and `backward()`
        # them to get the grads).
        grads, fetches = self.compute_gradients(postprocessed_batch)

        # Step the optimizer(s).
        for i in range(len(self._optimizers)):
            opt = self._optimizers[i]
            self._optimizers[i] = opt.apply_gradient(grads[i])

        # Update model's params.
        for k, v in self._optimizers[0].target.items():
            setattr(self.model, k, v)

        if self.model:
            fetches["model"] = self.model.metrics()
        return fetches

    @override(Policy)
    @DeveloperAPI
    def compute_gradients(self,
                          postprocessed_batch: SampleBatch) -> ModelGradients:
        # Get batch ready for RNNs, if applicable.
        pad_batch_to_sequences_of_same_size(
            postprocessed_batch,
            max_seq_len=self.max_seq_len,
            shuffle=False,
            batch_divisibility_req=self.batch_divisibility_req,
        )

        train_batch = self._lazy_tensor_dict(postprocessed_batch)
        model_params = self.model.variables(as_dict=True)

        # Calculate the actual policy loss.
        all_grads = force_list(
            self.gradient_loss({k: train_batch[k] for k in train_batch.keys() if k != "infos"}, model_params))#self, self.model, self.dist_class, train_batch,
                               #model_params))

        #remove: assert not any(torch.isnan(l) for l in loss_out)
        fetches = self.extra_compute_grad_fetches()

        # Loop through all optimizers.
        grad_info = {"allreduce_latency": 0.0}

        grad_info["allreduce_latency"] /= len(self._optimizers)
        grad_info.update(self.extra_grad_info(train_batch))

        return all_grads, dict(fetches, **{LEARNER_STATS_KEY: grad_info})

    @override(Policy)
    @DeveloperAPI
    def apply_gradients(self, gradients: ModelGradients) -> None:
        # TODO(sven): Not supported for multiple optimizers yet.
        assert len(self._optimizers) == 1

        # Step the optimizer(s).
        self._optimizers[0] = self._optimizers[0].apply_gradient(gradients)

    @override(Policy)
    @DeveloperAPI
    def get_weights(self) -> ModelWeights:
        cpu = jax.devices("cpu")[0]
        return {
            k: jax.device_put(v, cpu)
            for k, v in self.model.variables(as_dict=True).items()
        }

    @override(Policy)
    @DeveloperAPI
    def set_weights(self, weights: ModelWeights) -> None:
        for k, v in weights.items():
            setattr(self.model, k, v)

    @override(Policy)
    @DeveloperAPI
    def is_recurrent(self) -> bool:
        return len(self.model.get_initial_state()) > 0

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
            optim_state_dict = convert_to_non_jax_type(o.state_dict())
            state["_optimizer_variables"].append(optim_state_dict)
        return state

    @override(Policy)
    @DeveloperAPI
    def set_state(self, state: object) -> None:
        state = state.copy()  # shallow copy
        # Set optimizer vars first.
        optimizer_vars = state.pop("_optimizer_variables", None)
        if optimizer_vars:
            assert len(optimizer_vars) == len(self._optimizers)
            for i, (o, s) in enumerate(zip(self._optimizers, optimizer_vars)):
                self._optimizers[i].optimizer_def.state = s
        # Then the Policy's (NN) weights.
        super().set_state(state)

    @DeveloperAPI
    def extra_grad_process(self, optimizer: "jax.optim.Optimizer",
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
            state_batches: List[TensorType], model: JAXModelV2,
            action_dist: JAXDistribution) -> Dict[str, TensorType]:
        """Returns dict of extra info to include in experience batch.

        Args:
            input_dict (Dict[str, TensorType]): Dict of model input tensors.
            state_batches (List[TensorType]): List of state tensors.
            model (JAXModelV2): Reference to the model object.
            action_dist (JAXDistribution): Torch action dist object
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
    ) -> Union[List["flax.optim.Optimizer"], "flax.optim.Optimizer"]:
        """Custom the local FLAX optimizer(s) to use.

        Returns:
            Union[List[flax.optim.Optimizer], flax.optim.Optimizer]:
                The local FLAX optimizer(s) to use for this Policy.
        """
        if hasattr(self, "config"):
            adam = flax.optim.Adam(learning_rate=self.config["lr"])
        else:
            adam = flax.optim.Adam()
        weights = self.get_weights()
        adam_state = adam.init_state(weights)
        return flax.optim.Optimizer(adam, adam_state, target=weights)

    @override(Policy)
    @DeveloperAPI
    def export_model(self, export_dir: str) -> None:
        """TODO(sven): implement for JAX.
        """
        raise NotImplementedError

    @override(Policy)
    @DeveloperAPI
    def export_checkpoint(self, export_dir: str) -> None:
        """TODO(sven): implement for JAX.
        """
        raise NotImplementedError

    @override(Policy)
    @DeveloperAPI
    def import_model_from_h5(self, import_file: str) -> None:
        """Imports weights into JAX model."""
        return self.model.import_from_h5(import_file)

    def _lazy_tensor_dict(self, data):
        tensor_dict = UsageTrackingDict(data)
        tensor_dict.set_get_interceptor(
            functools.partial(convert_to_jax_device_array, device=None))
        return tensor_dict


# TODO: (sven) Unify hyperparam annealing procedures across RLlib (tf/torch)
#   and for all possible hyperparams, not just lr.
@DeveloperAPI
class LearningRateSchedule:
    """Mixin for TorchPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self.cur_lr = lr
        if lr_schedule is None:
            self.lr_schedule = ConstantSchedule(lr, framework=None)
        else:
            self.lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        self.cur_lr = self.lr_schedule.value(global_vars["timestep"])
        for i in range(len(self._optimizers)):
            opt = self._optimizers[i]
            new_hyperparams = opt.optimizer_def.update_hyper_params(
                learning_rate=self.cur_lr)
            opt.optimizer_def.hyper_params = new_hyperparams
