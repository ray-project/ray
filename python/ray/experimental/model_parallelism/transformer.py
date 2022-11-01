from transformers import (
    CONFIG_MAPPING,
    FLAX_MODEL_FOR_CAUSAL_LM_MAPPING,
    MODEL_CARD_NAME,
    AutoConfig,
    AutoTokenizer,
    FlaxAutoModelForCausalLM,
    HfArgumentParser,
    is_tensorboard_available,
    set_seed,
)
import jax.numpy as jnp

MODEL_CARD_NAME = "facebook/opt-125m"
LOCAL_CACHE_DIR = "/home/ray/llm_cache_dir"
REMOTE_CACHE_DIR = "/mnt/cluster_storage/llm_cache_dir"
DATASET_NAME = "wikitext"
DATASET_CONFIG_NAME = "wikitext-2-raw-v1"


# dataset

# config
config = AutoConfig.from_pretrained(
            MODEL_CARD_NAME,
            cache_dir=LOCAL_CACHE_DIR,
            use_auth_token=True,
        )
# tokenizer
tokenizer = AutoTokenizer.from_pretrained(
            MODEL_CARD_NAME,
            cache_dir=LOCAL_CACHE_DIR,
            use_auth_token=True,
        )
# model
model = FlaxAutoModelForCausalLM.from_pretrained(
            MODEL_CARD_NAME,
            config=config,
            seed=42,
            dtype=getattr(jnp, "float16"),
            use_auth_token=True,
        )

# training loop
