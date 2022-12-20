import gym
import numpy as np

from typing import (
    Dict,
    List,
    Tuple,
    Type,
    Union,
    Optional,
    Any,
    TYPE_CHECKING,
)

import tree
from gym.spaces import Discrete, Box

from ray.rllib.algorithms.dt.dt_torch_model import DTTorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.mingpt import configure_gpt_optimizer
from ray.rllib.models.torch.torch_action_dist import (
    TorchDistributionWrapper,
    TorchCategorical,
    TorchDeterministic,
)
from ray.rllib.policy.torch_mixins import LearningRateSchedule
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.threading import with_lock
from ray.rllib.utils.torch_utils import apply_grad_clipping
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
    TensorStructType,
    TensorShape,
)

if TYPE_CHECKING:
    from ray.rllib.evaluation import Episode  # noqa

torch, nn = try_import_torch()
F = nn.functional


class DTTorchPolicy(LearningRateSchedule, TorchPolicyV2):
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):
        LearningRateSchedule.__init__(
            self,
            config["lr"],
            config["lr_schedule"],
        )

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

    @override(TorchPolicyV2)
    def make_model_and_action_dist(
        self,
    ) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
        # Model
        model_config = self.config["model"]
        # TODO: make these better with better AlgorithmConfig options.
        model_config.update(
            embed_dim=self.config["embed_dim"],
            max_ep_len=self.config["horizon"],
            num_layers=self.config["num_layers"],
            num_heads=self.config["num_heads"],
            embed_pdrop=self.config["embed_pdrop"],
            resid_pdrop=self.config["resid_pdrop"],
            attn_pdrop=self.config["attn_pdrop"],
            use_obs_output=self.config.get("loss_coef_obs", 0) > 0,
            use_return_output=self.config.get("loss_coef_returns_to_go", 0) > 0,
        )

        num_outputs = int(np.product(self.observation_space.shape))

        model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=None,
            default_model=DTTorchModel,
            name="model",
        )

        # Action Distribution
        if isinstance(self.action_space, Discrete):
            action_dist = TorchCategorical
        elif isinstance(self.action_space, Box):
            action_dist = TorchDeterministic
        else:
            raise NotImplementedError

        return model, action_dist

    @override(TorchPolicyV2)
    def optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        optimizer = configure_gpt_optimizer(
            model=self.model,
            learning_rate=self.config["lr"],
            weight_decay=self.config["optimizer"]["weight_decay"],
            betas=self.config["optimizer"]["betas"],
        )

        return optimizer

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
        episode: Optional["Episode"] = None,
    ) -> SampleBatch:
        """Called by offline data reader after loading in one episode.
        Adds a done flag at the end of trajectory so that SegmentationBuffer can
        split using the done flag to avoid duplicate trajectories.
        """
        ep_len = sample_batch.env_steps()
        sample_batch[SampleBatch.DONES] = np.array([False] * (ep_len - 1) + [True])
        return sample_batch

    @PublicAPI
    def get_initial_input_dict(self, observation: TensorStructType) -> SampleBatch:
        """Get the initial input_dict to be passed into compute_single_action.

        Args:
            observation: first (unbatched) observation from env.reset()

        Returns:
            The input_dict for inference: {
                OBS: [max_seq_len, obs_dim] array,
                ACTIONS: [max_seq_len - 1, act_dim] array,
                RETURNS_TO_GO: [max_seq_len - 1] array,
                REWARDS: scalar,
                TIMESTEPS: [max_seq_len - 1] array,
            }
            Note the sequence lengths are different, and is specified as per
            view_requirements. Explanations in action_distribution_fn method.
        """
        observation = convert_to_numpy(observation)
        obs_shape = observation.shape
        obs_dtype = observation.dtype

        act_shape = self.action_space.shape
        act_dtype = self.action_space.dtype

        # Here we will pad all the required inputs to its proper sequence length
        # as their ViewRequirement.

        observations = np.concatenate(
            [
                np.zeros((self.max_seq_len - 1, *obs_shape), dtype=obs_dtype),
                observation[None],
            ],
            axis=0,
        )

        actions = np.zeros((self.max_seq_len - 1, *act_shape), dtype=act_dtype)

        rtg = np.zeros(self.max_seq_len - 1, dtype=np.float32)

        rewards = np.zeros((), dtype=np.float32)

        # -1 for masking in action_distribution_fn
        timesteps = np.full(self.max_seq_len - 1, fill_value=-1, dtype=np.int32)

        input_dict = SampleBatch(
            {
                SampleBatch.OBS: observations,
                SampleBatch.ACTIONS: actions,
                SampleBatch.RETURNS_TO_GO: rtg,
                SampleBatch.REWARDS: rewards,
                SampleBatch.T: timesteps,
            }
        )
        return input_dict

    @PublicAPI
    def get_next_input_dict(
        self,
        input_dict: SampleBatch,
        action: TensorStructType,
        reward: TensorStructType,
        next_obs: TensorStructType,
        extra: Dict[str, TensorType],
    ) -> SampleBatch:
        """Returns a new input_dict after stepping through the environment once.

        Args:
            input_dict: the input dict passed into compute_single_action.
            action: the (unbatched) action taken this step.
            reward: the (unbatched) reward from env.step
            next_obs: the (unbatached) next observation from env.step
            extra: the extra action out from compute_single_action.
                In this case contains current returns to go *before* the current
                reward is subtracted from target_return.

        Returns:
            A new input_dict to be passed into compute_single_action.
            The input_dict for inference: {
                OBS: [max_seq_len, obs_dim] array,
                ACTIONS: [max_seq_len - 1, act_dim] array,
                RETURNS_TO_GO: [max_seq_len - 1] array,
                REWARDS: scalar,
                TIMESTEPS: [max_seq_len - 1] array,
            }
            Note the sequence lengths are different, and is specified as per
            view_requirements. Explanations in action_distribution_fn method.
        """
        # creates a copy of input_dict with only numpy arrays
        input_dict = tree.map_structure(convert_to_numpy, input_dict)
        # convert everything else to numpy as well
        action, reward, next_obs, extra = convert_to_numpy(
            (action, reward, next_obs, extra)
        )

        # check dimensions
        assert input_dict[SampleBatch.OBS].shape == (
            self.max_seq_len,
            *self.observation_space.shape,
        )
        assert input_dict[SampleBatch.ACTIONS].shape == (
            self.max_seq_len - 1,
            *self.action_space.shape,
        )
        assert input_dict[SampleBatch.RETURNS_TO_GO].shape == (self.max_seq_len - 1,)
        assert input_dict[SampleBatch.T].shape == (self.max_seq_len - 1,)

        # Shift observations
        input_dict[SampleBatch.OBS] = np.concatenate(
            [
                input_dict[SampleBatch.OBS][1:],
                next_obs[None],
            ],
            axis=0,
        )

        # Shift actions
        input_dict[SampleBatch.ACTIONS] = np.concatenate(
            [
                input_dict[SampleBatch.ACTIONS][1:],
                action[None],
            ],
            axis=0,
        )

        # Reward is not a sequence, it's only used to calculate next rtg.
        input_dict[SampleBatch.REWARDS] = np.asarray(reward)

        # See methods action_distribution_fn and extra_action_out for an explanation
        # of why this is done.
        input_dict[SampleBatch.RETURNS_TO_GO] = np.concatenate(
            [
                input_dict[SampleBatch.RETURNS_TO_GO][1:],
                np.asarray(extra[SampleBatch.RETURNS_TO_GO])[None],
            ],
            axis=0,
        )

        # Shift and increment timesteps
        input_dict[SampleBatch.T] = np.concatenate(
            [
                input_dict[SampleBatch.T][1:],
                input_dict[SampleBatch.T][-1:] + 1,
            ],
            axis=0,
        )

        return input_dict

    @DeveloperAPI
    def get_initial_rtg_tensor(
        self,
        shape: TensorShape,
        dtype: Optional[Type] = torch.float32,
        device: Optional["torch.device"] = None,
    ):
        """Returns a initial/target returns-to-go tensor of the given shape.

        Args:
            shape: Shape of the rtg tensor.
            dtype: Type of the data in the tensor. Defaults to torch.float32.
            device: The device this tensor should be on. Defaults to self.device.
        """
        if device is None:
            device = self.device
        if dtype is None:
            device = torch.float32

        assert self.config["target_return"] is not None, "Must specify target_return."
        initial_rtg = torch.full(
            shape,
            fill_value=self.config["target_return"],
            dtype=dtype,
            device=device,
        )
        return initial_rtg

    @override(TorchPolicyV2)
    @DeveloperAPI
    def compute_actions(
        self,
        *args,
        **kwargs,
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:
        raise ValueError("Please use compute_actions_from_input_dict instead.")

    @override(TorchPolicyV2)
    def compute_actions_from_input_dict(
        self,
        input_dict: Union[SampleBatch, Dict[str, TensorStructType]],
        explore: bool = None,
        timestep: Optional[int] = None,
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        """
        Args:
            input_dict: input_dict (that contains a batch dimension for each value).
                Keys and shapes: {
                    OBS: [batch_size, max_seq_len, obs_dim],
                    ACTIONS: [batch_size, max_seq_len - 1, act_dim],
                    RETURNS_TO_GO: [batch_size, max_seq_len - 1],
                    REWARDS: [batch_size],
                    TIMESTEPS: [batch_size, max_seq_len - 1],
                }
            explore: unused.
            timestep: unused.
        Returns:
            A tuple consisting of a) actions, b) state_out, c) extra_fetches.
        """
        with torch.no_grad():
            # Pass lazy (torch) tensor dict to Model as `input_dict`.
            input_dict = input_dict.copy()
            input_dict = self._lazy_tensor_dict(input_dict)
            input_dict.set_training(True)

            actions, state_out, extra_fetches = self._compute_action_helper(input_dict)
            return actions, state_out, extra_fetches

    # TODO: figure out what this with_lock does and why it's only on the helper method.
    @with_lock
    @override(TorchPolicyV2)
    def _compute_action_helper(self, input_dict):
        # Switch to eval mode.
        self.model.eval()

        batch_size = input_dict[SampleBatch.OBS].shape[0]

        # NOTE: This is probably the most confusing part of the code, made to work with
        # env_runner and SimpleListCollector during evaluation, and thus should
        # be changed for the new Policy and Connector API.
        # So I'll explain how it works.

        # Add current timestep (+1 because -1 is first observation)
        # NOTE: ViewRequirement of timestep is -(max_seq_len-2):0.
        # The wierd limits is because RLlib treats initial obs as time -1,
        # and then 0 is (act, rew, next_obs), etc.
        # So we only collect max_seq_len-1 from the rollout and create the current
        # step here by adding 1.
        # Decision transformer treats initial observation as timestep 0, giving us
        # 0 is (obs, act, rew).
        timesteps = input_dict[SampleBatch.T]
        new_timestep = timesteps[:, -1:] + 1
        input_dict[SampleBatch.T] = torch.cat([timesteps, new_timestep], dim=1)

        # mask out any padded value at start of rollout
        # NOTE: the other reason for doing this is that evaluation rollout front
        # pads timesteps with -1, so using this we can find out when we need to mask
        # out the front section of the batch.
        input_dict[SampleBatch.ATTENTION_MASKS] = torch.where(
            input_dict[SampleBatch.T] >= 0, 1.0, 0.0
        )

        # Remove out-of-bound -1 timesteps after attention mask is calculated
        uncliped_timesteps = input_dict[SampleBatch.T]
        input_dict[SampleBatch.T] = torch.where(
            uncliped_timesteps < 0,
            torch.zeros_like(uncliped_timesteps),
            uncliped_timesteps,
        )

        # Computes returns-to-go.
        # NOTE: There are two rtg calculations: updated_rtg and initial_rtg.
        # updated_rtg takes the previous rtg value (the ViewRequirement is
        # -(max_seq_len-1):-1), and subtracts the last reward from it.
        rtg = input_dict[SampleBatch.RETURNS_TO_GO]
        last_rtg = rtg[:, -1]
        last_reward = input_dict[SampleBatch.REWARDS]
        updated_rtg = last_rtg - last_reward
        # initial_rtg simply is filled with target_return.
        # These two are both only for the current timestep.
        initial_rtg = self.get_initial_rtg_tensor(
            (batch_size, 1), dtype=rtg.dtype, device=rtg.device
        )

        # Then based on whether we are currently at the first timestep or not
        # we use the initial_rtg or updated_rtg.
        new_rtg = torch.where(new_timestep == 0, initial_rtg, updated_rtg[:, None])
        # Append the new_rtg to the batch.
        input_dict[SampleBatch.RETURNS_TO_GO] = torch.cat([rtg, new_rtg], dim=1)[
            ..., None
        ]

        # Pad current action (is not actually attended to and used during inference)
        past_actions = input_dict[SampleBatch.ACTIONS]
        action_pad = torch.zeros(
            (batch_size, 1, *past_actions.shape[2:]),
            dtype=past_actions.dtype,
            device=past_actions.device,
        )
        input_dict[SampleBatch.ACTIONS] = torch.cat([past_actions, action_pad], dim=1)

        # Run inference on model
        model_out, _ = self.model(input_dict)  # noop, just returns obs.
        preds = self.model.get_prediction(model_out, input_dict)
        dist_inputs = preds[SampleBatch.ACTIONS][:, -1]

        # Get the actions from the action_dist.
        action_dist = self.dist_class(dist_inputs, self.model)
        actions = action_dist.deterministic_sample()

        # This is used by env_runner and is actually how it adds custom keys to
        # SimpleListCollector and allows ViewRequirements to work.
        # This is also used in user inference in get_next_input_dict, which takes
        # this output as one of the input.
        extra_fetches = {
            # new_rtg still has the leftover extra 3rd dimension for inference
            SampleBatch.RETURNS_TO_GO: new_rtg.squeeze(-1),
            SampleBatch.ACTION_DIST_INPUTS: dist_inputs,
        }

        # Update our global timestep by the batch size.
        self.global_timestep += len(input_dict[SampleBatch.CUR_OBS])

        return convert_to_numpy((actions, [], extra_fetches))

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Loss function.

        Args:
            model: The ModelV2 to run foward pass on.
            dist_class: The distribution of this policy.
            train_batch: Training SampleBatch.
                Keys and shapes: {
                    OBS: [batch_size, max_seq_len, obs_dim],
                    ACTIONS: [batch_size, max_seq_len, act_dim],
                    RETURNS_TO_GO: [batch_size, max_seq_len + 1, 1],
                    TIMESTEPS: [batch_size, max_seq_len],
                    ATTENTION_MASKS: [batch_size, max_seq_len],
                }
        Returns:
            Loss scalar tensor.
        """
        train_batch = self._lazy_tensor_dict(train_batch)

        # policy forward and get prediction
        model_out, _ = self.model(train_batch)  # noop, just returns obs.
        preds = self.model.get_prediction(model_out, train_batch)

        # get the regression targets
        targets = self.model.get_targets(model_out, train_batch)

        # get the attention masks for masked-loss
        masks = train_batch[SampleBatch.ATTENTION_MASKS]

        # compute loss
        loss = self._masked_loss(preds, targets, masks)

        self.log("cur_lr", torch.tensor(self.cur_lr))

        return loss

    def _masked_loss(self, preds, targets, masks):
        losses = []
        for key in targets:
            assert (
                key in preds
            ), "for target {key} there is no prediction from the output of the model"
            loss_coef = self.config.get(f"loss_coef_{key}", 1.0)
            if self._is_discrete(key):
                loss = loss_coef * self._masked_cross_entropy_loss(
                    preds[key], targets[key], masks
                )
            else:
                loss = loss_coef * self._masked_mse_loss(
                    preds[key], targets[key], masks
                )

            losses.append(loss)
            self.log(f"{key}_loss", loss)

        return sum(losses)

    def _is_discrete(self, key):
        return key == SampleBatch.ACTIONS and isinstance(self.action_space, Discrete)

    def _masked_cross_entropy_loss(
        self,
        preds: TensorType,
        targets: TensorType,
        masks: TensorType,
    ) -> TensorType:
        """Computes cross-entropy loss between preds and targets, subject to a mask.

        Args:
            preds: logits of shape [B1, ..., Bn, M]
            targets: index targets for preds of shape [B1, ..., Bn]
            masks: 0 means don't compute loss, 1 means compute loss
                shape [B1, ..., Bn]

        Returns:
            Scalar cross entropy loss.
        """
        losses = F.cross_entropy(
            preds.reshape(-1, preds.shape[-1]),
            targets.reshape(-1).long(),
            reduction="none",
        )
        losses = losses * masks.reshape(-1)
        return losses.mean()

    def _masked_mse_loss(
        self,
        preds: TensorType,
        targets: TensorType,
        masks: TensorType,
    ) -> TensorType:
        """Computes MSE loss between preds and targets, subject to a mask.

        Args:
            preds: logits of shape [B1, ..., Bn, M]
            targets: index targets for preds of shape [B1, ..., Bn]
            masks: 0 means don't compute loss, 1 means compute loss
                shape [B1, ..., Bn]

        Returns:
            Scalar cross entropy loss.
        """
        losses = F.mse_loss(preds, targets, reduction="none")
        losses = losses * masks.reshape(
            *masks.shape, *([1] * (len(preds.shape) - len(masks.shape)))
        )
        return losses.mean()

    @override(TorchPolicyV2)
    def extra_grad_process(self, local_optimizer, loss):
        return apply_grad_clipping(self, local_optimizer, loss)

    def log(self, key, value):
        # internal log function
        self.model.tower_stats[key] = value

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        stats_dict = {
            k: torch.stack(self.get_tower_stats(k)).mean().item()
            for k in self.model.tower_stats
        }
        return stats_dict
