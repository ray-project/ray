import logging
from typing import List, Optional, Type, Tuple

from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.algorithms.dt.segmentation_buffer import SegmentationBuffer
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
)
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    PartialAlgorithmConfigDict,
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
        self.train_batch_size = 1
        self.shuffle_buffer_size = 32
        self.max_seq_len = 20
        self.lr = 1e-4
        self.max_ep_len = self.horizon

        # __sphinx_doc_end__
        # fmt: on
        self.weight_decay = 1e-4
        self.betas = (0.9, 0.95)

        self.embed_dim = 128
        self.num_layers = 3
        self.num_heads = 1
        self.embed_pdrop = 0.1
        self.resid_pdrop = 0.1
        self.attn_pdrop = 0.1
        self.use_obs_output = False
        self.use_return_output = False
        self.target_return = None

        # overriding the trainer config default
        # If data ingestion/sample_time is slow, increase this
        self.num_workers = 0
        self.offline_sampling = True
        self.min_iter_time_s = 10.0
        self.postprocess_inputs = True

    def training(
        self,
        *,
        shuffle_buffer_size: Optional[int] = None,
        weight_decay: Optional[float] = None,
        betas: Optional[Tuple[float, float]] = None,
        max_seq_len: Optional[int] = None,
        max_ep_len: Optional[int] = None,
        embed_dim: Optional[int] = None,
        num_layers: Optional[int] = None,
        num_heads: Optional[int] = None,
        embed_pdrop: Optional[float] = None,
        resid_pdrop: Optional[float] = None,
        attn_pdrop: Optional[float] = None,
        use_obs_output: Optional[bool] = None,
        use_return_output: Optional[bool] = None,
        target_return: Optional[float] = None,
        **kwargs,
    ) -> "DTConfig":
        """
        === DT configs

        Args:
            **kwargs: forward compatibility kwargs
            weight_decay: weight decay for AdamW optimizer

        Returns:
            This updated CRRConfig object.
        """
        # TODO(charlesjsun): finish doc

        super().training(**kwargs)
        if shuffle_buffer_size is not None:
            self.shuffle_buffer_size = shuffle_buffer_size
        if weight_decay is not None:
            self.weight_decay = weight_decay
        if betas is not None:
            self.betas = betas
        if max_seq_len is not None:
            self.max_seq_len = max_seq_len
        if max_ep_len is not None:
            self.max_ep_len = max_ep_len
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

        return self


class DT(Algorithm):

    # TODO: we have a circular dependency for get
    #  default config. config -> Trainer -> config
    #  defining Config class in the same file for now as a workaround.

    def setup(self, config: PartialAlgorithmConfigDict):
        super().setup(config)

        # TODO(charlesjsun): add heuristics log2(dataset_size)
        self.buffer = SegmentationBuffer(
            self.config["shuffle_buffer_size"],
            self.config["max_seq_len"],
        )

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
            self.config["target_return"] is not None
        ), "Must specify a target return (total sum of rewards)."

        assert self.config["max_seq_len"] >= 2, "max_seq_len must be at least 2."

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
        # train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # TODO(charlesjsun): better control over batch sizes
        batch_size = train_batch[SampleBatch.OBS].shape[0]

        # TODO(charlesjsun): Action normalization?

        self.buffer.add(train_batch)
        train_batch = self.buffer.sample(batch_size)

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

        return train_results
