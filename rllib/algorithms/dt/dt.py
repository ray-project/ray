import logging
import math
from typing import List, Optional, Type, Tuple, Dict, Any, Union

from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dt.segmentation_buffer import MultiAgentSegmentationBuffer
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.typing import (
    ResultDict,
    TensorStructType,
    PolicyID,
    TensorType,
)

logger = logging.getLogger(__name__)


class DTConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or DT)

        # fmt: off
        # __sphinx_doc_begin__
        # DT-specific settings.
        # Required settings during training and evaluation:
        # Initial return to go used as target during rollout.
        self.target_return = None
        # Rollout horizon/maximum episode length.
        self.horizon = None

        # Model settings:
        self.model = {
            # Transformer (GPT) context length.
            "max_seq_len": 5,
        }

        # Transformer (GPT) settings:
        self.embed_dim = 128
        self.num_layers = 2
        self.num_heads = 1
        self.embed_pdrop = 0.1
        self.resid_pdrop = 0.1
        self.attn_pdrop = 0.1

        # Optimization settings:
        self.lr = 1e-4
        self.lr_schedule = None
        self.optimizer = {
            # Weight decay for Adam optimizer.
            "weight_decay": 1e-4,
            # Betas for Adam optimizer.
            "betas": (0.9, 0.95),
        }
        self.grad_clip = None
        # Coefficients on the loss for each of the heads.
        # By default, only use the actions outputs for training.
        self.loss_coef_actions = 1
        self.loss_coef_obs = 0
        self.loss_coef_returns_to_go = 0

        self.replay_buffer_config = {
            # How many trajectories/episodes does the segmentation buffer hold.
            # Increase for more data shuffling but increased memory usage.
            "capacity": 20,
            # Do not change the type of replay buffer.
            "type": MultiAgentSegmentationBuffer,
        }
        # __sphinx_doc_end__
        # fmt: on

        # Overwriting the Algorithm config default
        # Number of training_step calls between evaluation rollouts.
        self.min_train_timesteps_per_iteration = 5000

        # Don't change
        self.offline_sampling = True
        self.postprocess_inputs = True
        self.discount = None

    def training(
        self,
        *,
        replay_buffer_config: Optional[Dict[str, Any]] = NotProvided,
        embed_dim: Optional[int] = NotProvided,
        num_layers: Optional[int] = NotProvided,
        num_heads: Optional[int] = NotProvided,
        embed_pdrop: Optional[float] = NotProvided,
        resid_pdrop: Optional[float] = NotProvided,
        attn_pdrop: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        loss_coef_actions: Optional[float] = NotProvided,
        loss_coef_obs: Optional[float] = NotProvided,
        loss_coef_returns_to_go: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        horizon: Optional[int] = NotProvided,
        **kwargs,
    ) -> "DTConfig":
        """
        === DT configs

        Args:
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "capacity": 50000,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            embed_dim: Dimension of the embeddings in the GPT model.
            num_layers: Number of attention layers in the GPT model.
            num_heads: Number of attention heads in the GPT model. Must divide
                embed_dim evenly.
            embed_pdrop: Dropout probability of the embedding layer of the GPT model.
            resid_pdrop: Dropout probability of the residual layer of the GPT model.
            attn_pdrop: Dropout probability of the attention layer of the GPT model.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            loss_coef_actions: Coefficients on the loss for the actions output.
                Default to 1.
            loss_coef_obs: Coefficients on the loss for the obs output. Default to 0.
                Set to a value greater than 0 to regress on the obs output.
            loss_coef_returns_to_go: Coefficients on the loss for the returns_to_go
                output. Default to 0. Set to a value greater than 0 to regress on the
                returns_to_go output.
            horizon: The episode horizon used. This value can be derived from your
                environment via `[your_env]._max_episode_steps`.
            **kwargs: Forward compatibility kwargs

        Returns:
            This updated DTConfig object.
        """
        super().training(**kwargs)
        if replay_buffer_config is not NotProvided:
            # Override entire `replay_buffer_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_replay_buffer_config = deep_update(
                {"replay_buffer_config": self.replay_buffer_config},
                {"replay_buffer_config": replay_buffer_config},
                False,
                ["replay_buffer_config"],
                ["replay_buffer_config"],
            )
            self.replay_buffer_config = new_replay_buffer_config["replay_buffer_config"]
        if embed_dim is not NotProvided:
            self.embed_dim = embed_dim
        if num_layers is not NotProvided:
            self.num_layers = num_layers
        if num_heads is not NotProvided:
            self.num_heads = num_heads
        if embed_pdrop is not NotProvided:
            self.embed_pdrop = embed_pdrop
        if resid_pdrop is not NotProvided:
            self.resid_pdrop = resid_pdrop
        if attn_pdrop is not NotProvided:
            self.attn_pdrop = attn_pdrop
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if loss_coef_actions is not NotProvided:
            self.loss_coef_actions = loss_coef_actions
        if loss_coef_obs is not NotProvided:
            self.loss_coef_obs = loss_coef_obs
        if loss_coef_returns_to_go is not NotProvided:
            self.loss_coef_returns_to_go = loss_coef_returns_to_go
        if horizon is not NotProvided:
            self.horizon = horizon

        return self

    def evaluation(
        self,
        *,
        target_return: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DTConfig":
        """
        === DT configs

        Args:
            target_return: The target return-to-go for inference/evaluation.
            **kwargs: Forward compatibility kwargs

        Returns:
            This updated DTConfig object.
        """
        super().evaluation(**kwargs)
        if target_return is not NotProvided:
            self.target_return = target_return

        return self

    @override(AlgorithmConfig)
    def rollouts(self, *args, **kwargs):
        if "horizon" in kwargs:
            raise ValueError(
                "`horizon` setting no longer supported via "
                "`config.rollouts(horizon=..)`! This is a DT-only setting now and "
                "must be specified via `config.training(horizon=..)`."
            )
        return super().rollouts(*args, **kwargs)

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # target_return must be specified
        assert (
            self.target_return is not None
        ), "Must specify a target return (total sum of rewards)."

        # horizon must be specified and >= 2
        assert self.horizon is not None, "Must specify rollout horizon."
        assert self.horizon >= 2, "rollout horizon must be at least 2."

        # replay_buffer's type must be MultiAgentSegmentationBuffer
        assert (
            self.replay_buffer_config is not None
        ), "Must specify replay_buffer_config."
        replay_buffer_type = self.replay_buffer_config.get("type")
        assert (
            replay_buffer_type == MultiAgentSegmentationBuffer
        ), "replay_buffer's type must be MultiAgentSegmentationBuffer."

        # max_seq_len must be specified in model
        model_max_seq_len = self.model.get("max_seq_len")
        assert model_max_seq_len is not None, "Must specify model's max_seq_len."

        # User shouldn't need to specify replay_buffer's max_seq_len.
        # Autofill for replay buffer API. If they did specify, make sure it
        # matches with model's max_seq_len
        buffer_max_seq_len = self.replay_buffer_config.get("max_seq_len")
        if buffer_max_seq_len is None:
            self.replay_buffer_config["max_seq_len"] = model_max_seq_len
        else:
            assert (
                buffer_max_seq_len == model_max_seq_len
            ), "replay_buffer's max_seq_len must equal model's max_seq_len."

        # Same thing for buffer's max_ep_len, which should be autofilled from
        # rollout's horizon, or check that it matches if user specified.
        buffer_max_ep_len = self.replay_buffer_config.get("max_ep_len")
        if buffer_max_ep_len is None:
            self.replay_buffer_config["max_ep_len"] = self.horizon
        else:
            assert (
                buffer_max_ep_len == self.horizon
            ), "replay_buffer's max_ep_len must equal rollout horizon."


@Deprecated(
    old="rllib/algorithms/dt/",
    new="rllib_contrib/dt/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class DT(Algorithm):
    """Implements Decision Transformer: https://arxiv.org/abs/2106.01345."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return DTConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.dt.dt_torch_policy import DTTorchPolicy

            return DTTorchPolicy
        else:
            raise ValueError("Non-torch frameworks are not supported yet!")

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        with self._timers[SAMPLE_TIMER]:
            # TODO: Add ability to do obs_filter for offline sampling.
            train_batch = synchronous_parallel_sample(worker_set=self.workers)

        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Because each sample is a segment of max_seq_len transitions, doing
        # the division makes it so the total number of transitions per train
        # step is consistent.
        num_steps = train_batch.env_steps()
        batch_size = int(math.ceil(num_steps / self.config.model["max_seq_len"]))

        # Add the batch of episodes to the segmentation buffer.
        self.local_replay_buffer.add(train_batch)
        # Sample a batch of segments.
        train_batch = self.local_replay_buffer.sample(batch_size)

        # Postprocess batch before we learn on it.
        post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
        train_batch = post_fn(train_batch, self.workers, self.config)

        # Learn on training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer", False):
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update learning rate scheduler.
        global_vars = {
            # Note: this counts the number of segments trained, not timesteps.
            # i.e. NUM_AGENT_STEPS_TRAINED: B, NUM_AGENT_STEPS_SAMPLED: B*T
            "timestep": self._counters[NUM_AGENT_STEPS_TRAINED],
        }
        self.workers.local_worker().set_global_vars(global_vars)

        return train_results

    @PublicAPI
    @override(Algorithm)
    def compute_single_action(
        self,
        *args,
        input_dict: Optional[SampleBatch] = None,
        full_fetch: bool = True,
        **kwargs,
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:
        """Computes an action for the specified policy on the local worker.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_single_action() on it
        directly.

        Args:
            input_dict: A SampleBatch taken from get_initial_input_dict or
                get_next_input_dict.
            full_fetch: Whether to return extra action fetch results.
                This is always True for DT.
            kwargs: forward compatibility args.

        Returns:
            A tuple containing: (
                the computed action,
                list of RNN states (empty for DT),
                extra action output (pass to get_next_input_dict),
            )
        """
        assert input_dict is not None, (
            "DT must take in input_dict for inference. "
            "See get_initial_input_dict() and get_next_input_dict()."
        )
        assert (
            full_fetch
        ), "DT needs full_fetch=True. Pass extra into get_next_input_dict()."

        return super().compute_single_action(
            *args, input_dict=input_dict.copy(), full_fetch=full_fetch, **kwargs
        )

    @PublicAPI
    def get_initial_input_dict(
        self,
        observation: TensorStructType,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> SampleBatch:
        """Get the initial input_dict to be passed into compute_single_action.

        Args:
            observation: first (unbatched) observation from env.reset()
            policy_id: Policy to query (only applies to multi-agent).
                Default: "default_policy".

        Returns:
            The input_dict for inference.
        """
        policy = self.get_policy(policy_id)
        return policy.get_initial_input_dict(observation)

    @PublicAPI
    def get_next_input_dict(
        self,
        input_dict: SampleBatch,
        action: TensorStructType,
        reward: TensorStructType,
        next_obs: TensorStructType,
        extra: Dict[str, TensorType],
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> SampleBatch:
        """Returns a new input_dict after stepping through the environment once.

        Args:
            input_dict: the input dict passed into compute_single_action.
            action: the (unbatched) action taken this step.
            reward: the (unbatched) reward from env.step
            next_obs: the (unbatached) next observation from env.step
            extra: the extra action out from compute_single_action.
                For DT this case contains current returns to go *before* the current
                reward is subtracted from target_return.
            policy_id: Policy to query (only applies to multi-agent).
                Default: "default_policy".

        Returns:
            A new input_dict to be passed into compute_single_action.
        """
        policy = self.get_policy(policy_id)
        return policy.get_next_input_dict(input_dict, action, reward, next_obs, extra)
