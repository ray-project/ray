from typing import Union, Dict
from collections import UserDict

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
from transformers.testing_utils import CaptureLogger
import pandas as pd
import numpy as np
import jax.numpy as jnp

import ipdb
import ray
from transformers.tokenization_utils_base import BatchEncoding

MODEL_CARD_NAME = "facebook/opt-125m"
LOCAL_CACHE_DIR = "/home/ray/llm_cache_dir"
REMOTE_CACHE_DIR = "/mnt/cluster_storage/llm_cache_dir"
DATASET_NAME = "wikitext"
DATASET_CONFIG_NAME = "wikitext-2-raw-v1"


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
            cache_dir=LOCAL_CACHE_DIR,
            seed=42,
            dtype=getattr(jnp, "float16"),
            use_auth_token=True,
        )

# dataset
# TODO: tokenize, group
ds: ray.data.Dataset = ray.data.read_parquet(
    "/mnt/cluster_storage/bigcode/split_parquet_2_shards"
).repartition(32)  # take advantage of num_cpus

# Preprocessing
column_names = ds.schema().names
text_column_name = "content"

def tokenize_function(examples: pd.DataFrame):
    input_content_list = examples[text_column_name].tolist()
    tokenized_output: Union[BatchEncoding, UserDict] = tokenizer(input_content_list)
    # <class 'transformers.tokenization_utils_base.BatchEncoding'>
    # dict_keys(['input_ids', 'attention_mask'])
    return {k: np.array(v) for k, v in tokenized_output.items()}

# Output block format is Arrow
ds = ds.select_columns(cols=[text_column_name]).map_batches(tokenize_function)

print(ds)
ipdb.set_trace()
# training loop
