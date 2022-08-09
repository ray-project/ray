import logging
import math
from typing import List, Optional, Type, Tuple, Dict, Any, Union

from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.algorithms.dt.segmentation_buffer import (
    SegmentationBuffer,
    MultiAgentSegmentationBuffer,
)
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    ResultDict,
)

logger = logging.getLogger(__name__)


class DTConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or DT)

        # fmt: off
        # __sphinx_doc_begin__
        # DT-specific settings.
        # TODO(charlesjsun): what is sphinx_doc
        self.lr = 1e-4
        self.lr_schedule = None
        self.optimizer = {
            "weight_decay": 1e-4,
            "betas": (0.9, 0.95),
        }
        self.grad_clip = None

        self.model = {
            "max_seq_len": 20,
        }

        self.embed_dim = 128
        self.num_layers = 3
        self.num_heads = 1
        self.embed_pdrop = 0.1
        self.resid_pdrop = 0.1
        self.attn_pdrop = 0.1
        self.use_obs_output = False
        self.use_return_output = False
        self.target_return = None

        # TODO(charlesjsun): Dont' change type doc, also the other two are auto filled
        self.replay_buffer_config = {
            "type": MultiAgentSegmentationBuffer,
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

        # TODO(charlesjsun): discount = None

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
        use_obs_output: Optional[bool] = None,
        use_return_output: Optional[bool] = None,
        target_return: Optional[float] = None,
        grad_clip: Optional[float] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        **kwargs,
    ) -> "DTConfig":
        """
        === DT configs

        Args:
            **kwargs: forward compatibility kwargs

        Returns:
            This updated CRRConfig object.
        """
        # TODO(charlesjsun): finish doc

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
        if use_obs_output is not None:
            self.use_obs_output = use_obs_output
        if use_return_output is not None:
            self.use_return_output = use_return_output
        if target_return is not None:
            self.target_return = target_return
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule

        return self


class DT(Algorithm):

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

        assert (
            self.config.get("target_return") is not None
        ), "Must specify a target return (total sum of rewards)."

        assert self.config.get("horizon") is not None, "Must specify rollout horizon."
        assert self.config["horizon"] >= 2, "rollout horizon must be at least 2."

        assert (
            self.config.get("replay_buffer_config") is not None
        ), "Must specify replay_buffer_config."
        replay_buffer_type = self.config["replay_buffer_config"].get("type")
        assert (
            replay_buffer_type == MultiAgentSegmentationBuffer
        ), "replay_buffer's type must be MultiAgentSegmentationBuffer."

        model_max_seq_len = self.config["model"].get("max_seq_len")
        assert model_max_seq_len is not None, "Must specify model's max_seq_len."

        buffer_max_seq_len = self.config["replay_buffer_config"].get("max_seq_len")
        if buffer_max_seq_len is None:
            self.config["replay_buffer_config"]["max_seq_len"] = model_max_seq_len
        else:
            assert (
                buffer_max_seq_len == model_max_seq_len
            ), "replay_buffer's max_seq_len must equal model's max_seq_len."

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
            train_batch = synchronous_parallel_sample(worker_set=self.workers)
        # TODO(charlesjsun): Fix multiagent later
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        num_steps = train_batch.env_steps()
        batch_size = int(math.ceil(num_steps / self.config["model"]["max_seq_len"]))

        self.local_replay_buffer.add(train_batch)
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

        global_vars = {
            # Note: this counts the number of segments trained, not timesteps.
            # i.e. NUM_AGENT_STEPS_TRAINED: B, NUM_AGENT_STEPS_SAMPLED: B*T
            "timestep": self._counters[NUM_AGENT_STEPS_TRAINED],
        }
        self.workers.local_worker().set_global_vars(global_vars)

        return train_results
