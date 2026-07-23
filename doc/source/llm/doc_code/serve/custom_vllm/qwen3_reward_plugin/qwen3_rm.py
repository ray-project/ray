"""Qwen3 with the LM head replaced by a scalar reward head, as a vLLM plugin."""

import os
from collections.abc import Iterable

import torch

from vllm.config import VllmConfig
from vllm.logger import init_logger
from vllm.model_executor.layers.linear import ReplicatedLinear
from vllm.model_executor.layers.pooler import DispatchPooler
from vllm.model_executor.model_loader.weight_utils import default_weight_loader
from vllm.model_executor.models.interfaces_base import default_pooling_type
from vllm.model_executor.models.qwen3 import Qwen3ForCausalLM
from vllm.model_executor.models.utils import AutoWeightsLoader, maybe_prefix

logger = init_logger(__name__)

REWARD_HEAD_FILE = "reward_head.pt"
REWARD_HEAD_KEYS = ("linear.weight", "weight", "score.weight")


@default_pooling_type(seq_pooling_type="LAST")
class Qwen3CustomRewardModel(Qwen3ForCausalLM):
    """Qwen3 backbone + scalar reward head, served as a pooling model."""

    is_pooling_model = True

    def __init__(self, *, vllm_config: VllmConfig, prefix: str = ""):
        # A single scalar output (set problem_type="regression" in the config
        # for an identity activation, so /classify returns the raw reward).
        vllm_config.model_config.hf_config.num_labels = 1
        super().__init__(vllm_config=vllm_config, prefix=prefix)

        model_config = vllm_config.model_config

        # Scalar reward head. Replicated because the output dim is 1.
        self.score = ReplicatedLinear(
            model_config.hf_config.hidden_size,
            1,
            bias=False,
            params_dtype=model_config.head_dtype,
            return_bias=False,
            prefix=maybe_prefix(prefix, "score"),
        )

        # LAST-token pooling -> reward head -> raw score. Advertising the
        # "classify" task mounts vLLM's /classify route.
        pooler_config = model_config.pooler_config
        assert pooler_config is not None
        self.pooler = DispatchPooler.for_seq_cls(pooler_config, classifier=self.score)

    def load_weights(self, weights: Iterable[tuple[str, torch.Tensor]]) -> set[str]:
        skip_prefixes = ["score."]  # reward head loads separately, below
        if self.config.tie_word_embeddings:
            skip_prefixes.append("lm_head.")  # tied: no lm_head weight to load
        loader = AutoWeightsLoader(self, skip_prefixes=skip_prefixes)
        loaded = loader.load_weights(weights)
        if self._load_reward_head():
            loaded.add("score.weight")
        return loaded

    def _load_reward_head(self) -> bool:
        """Load ``reward_head.pt``; return False (keep init values) if absent."""
        env_path = os.environ.get("RM_REWARD_HEAD_PATH")
        model_dir = self.vllm_config.model_config.model
        head_path = (
            env_path
            if env_path and os.path.isfile(env_path)
            else os.path.join(model_dir, REWARD_HEAD_FILE)
        )
        if not os.path.isfile(head_path):
            logger.warning(
                "Reward head %r not found; scores are not meaningful.", head_path
            )
            return False

        state = torch.load(head_path, map_location="cpu", weights_only=True)
        weight = (
            state
            if isinstance(state, torch.Tensor)
            else next((state[k] for k in REWARD_HEAD_KEYS if k in state), None)
        )
        if weight is None:
            raise KeyError(f"{REWARD_HEAD_FILE} has none of {REWARD_HEAD_KEYS}")

        param = self.score.weight
        weight_loader = getattr(param, "weight_loader", default_weight_loader)
        weight_loader(param, weight.reshape(1, -1).to(param.dtype))
        logger.info("Loaded reward head from %s.", head_path)
        return True
