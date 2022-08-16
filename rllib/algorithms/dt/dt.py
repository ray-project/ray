import logging
import math
from typing import List, Optional, Type, Tuple, Dict, Any, Union

from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.algorithms.dt.segmentation_buffer import MultiAgentSegmentationBuffer
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
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
        self.target_return = None
        self.horizon = None
        self.model = {
            "max_seq_len": 5,
        }

        self.embed_dim = 128
        self.num_layers = 2
        self.num_heads = 1
        self.embed_pdrop = 0.1
        self.resid_pdrop = 0.1
        self.attn_pdrop = 0.1

        self.lr = 1e-4
        self.lr_schedule = None
        self.optimizer = {
            # Weight decay for Adam optimizer.
            "weight_decay": 1e-4,
            # Betas for Adam optimizer.
            "betas": (0.9, 0.95),
        }
        self.grad_clip = None

        self.replay_buffer_config = {
            # Do not change the type of replay buffer.
            "type": MultiAgentSegmentationBuffer,
            # How many trajectories/episodes does the buffer hold.
            "capacity": 20,
        }

        # __sphinx_doc_end__
        # fmt: on

        # overriding the trainer config default
        # If data ingestion/sample_time is slow, increase this
        self.num_workers = 0
        self.min_time_s_per_iteration = 10.0

        # Don't change
        self.offline_sampling = True
        self.postprocess_inputs = True
        self.discount = None

    def training(
        self,
        *,
        replay_buffer_config: Optional[Dict[str, Any]],
        embed_dim: Optional[int] = None,
        num_layers: Optional[int] = None,
        num_heads: Optional[int] = None,
        embed_pdrop: Optional[float] = None,
        resid_pdrop: Optional[float] = None,
        attn_pdrop: Optional[float] = None,
        target_return: Optional[float] = None,
        grad_clip: Optional[float] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        **kwargs,
    ) -> "DTConfig":
        """
        === DT configs

        Args:
            replay_buffer_config: Replay buffer config.
                {
                    "capacity": How many trajectories/episodes does the buffer hold.
                }
            embed_dim: Dimension of the embeddings in the GPT model.
            num_layers: Number of attention layers in the GPT model.
            num_heads: Number of attention heads in the GPT model. Must divide
                embed_dim evenly.
            embed_pdrop: Dropout probability of the embedding layer of the GPT model.
            resid_pdrop: Dropout probability of the residual layer of the GPT model.
            attn_pdrop: Dropout probability of the attention layer of the GPT model.
            target_return: The target return-to-go for inference/evaluation.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            **kwargs: Forward compatibility kwargs

        Returns:
            This updated DTConfig object.
        """
        super().training(**kwargs)
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if embed_dim is not None:
            self.embed_dim = embed_dim
        if num_layers is not None:
            self.num_layers = num_layers
        if num_heads is not None:
            self.num_heads = num_heads
        if embed_pdrop is not None:
            self.embed_pdrop = embed_pdrop
        if resid_pdrop is not None:
            self.resid_pdrop = resid_pdrop
        if attn_pdrop is not None:
            self.attn_pdrop = attn_pdrop
        if target_return is not None:
            self.target_return = target_return
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule

        return self


class DT(Algorithm):
    """Implements Decision Transformer: https://arxiv.org/abs/2106.01345"""

    # TODO: we have a circular dependency for get
    #  default config. config -> Trainer -> config
    #  defining Config class in the same file for now as a workaround.

    @override(Algorithm)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
        """Validates the Trainer's config dict.

        Args:
            config: The Trainer's config to check.

        Raises:
            ValueError: In case something is wrong with the config.
        """
        # Call super's validation method.
        super().validate_config(config)

        # target_return must be specified
        assert (
            self.config.get("target_return") is not None
        ), "Must specify a target return (total sum of rewards)."

        # horizon must be specified and >= 2
        assert self.config.get("horizon") is not None, "Must specify rollout horizon."
        assert self.config["horizon"] >= 2, "rollout horizon must be at least 2."

        # replay_buffer's type must be MultiAgentSegmentationBuffer
        assert (
            self.config.get("replay_buffer_config") is not None
        ), "Must specify replay_buffer_config."
        replay_buffer_type = self.config["replay_buffer_config"].get("type")
        assert (
            replay_buffer_type == MultiAgentSegmentationBuffer
        ), "replay_buffer's type must be MultiAgentSegmentationBuffer."

        # max_seq_len must be specified in model
        model_max_seq_len = self.config["model"].get("max_seq_len")
        assert model_max_seq_len is not None, "Must specify model's max_seq_len."

        # User shouldn't need to specify replay_buffer's max_seq_len.
        # Autofill for replay buffer API. If they did specify, make sure it
        # matches with model's max_seq_len
        buffer_max_seq_len = self.config["replay_buffer_config"].get("max_seq_len")
        if buffer_max_seq_len is None:
            self.config["replay_buffer_config"]["max_seq_len"] = model_max_seq_len
        else:
            assert (
                buffer_max_seq_len == model_max_seq_len
            ), "replay_buffer's max_seq_len must equal model's max_seq_len."

        # Same thing for buffer's max_ep_len, which should be autofilled from
        # rollout's horizon, or check that it matches if user specified.
        buffer_max_ep_len = self.config["replay_buffer_config"].get("max_ep_len")
        if buffer_max_ep_len is None:
            self.config["replay_buffer_config"]["max_ep_len"] = self.config["horizon"]
        else:
            assert (
                buffer_max_ep_len == self.config["horizon"]
            ), "replay_buffer's max_ep_len must equal rollout horizon."

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return DTConfig().to_dict()

    @override(Algorithm)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
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
        batch_size = int(math.ceil(num_steps / self.config["model"]["max_seq_len"]))

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
