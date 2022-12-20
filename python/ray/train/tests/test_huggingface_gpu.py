import logging
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from ray.train.batch_predictor import BatchPredictor
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer


import ray
from ray.train.huggingface import HuggingFaceCheckpoint, HuggingFacePredictor

test_strings = ["Complete me", "And me", "Please complete"]
prompts = pd.DataFrame(test_strings, columns=["sentences"])

# We are only testing Causal Language Modeling here

model_checkpoint = "hf-internal-testing/tiny-random-gpt2"
tokenizer_checkpoint = "hf-internal-testing/tiny-random-gpt2"


def create_checkpoint():
    with tempfile.TemporaryDirectory() as tmpdir:
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
        checkpoint = HuggingFaceCheckpoint.from_model(model, tokenizer, path=tmpdir)
        # Serialize to dict so we can remove the temporary directory
        return HuggingFaceCheckpoint.from_dict(checkpoint.to_dict())


# TODO(ml-team): Add np.ndarray to batch_type
@pytest.mark.parametrize("batch_type", [pd.DataFrame])
@pytest.mark.parametrize("device", [None, 0])
def test_predict_batch(ray_start_4_cpus, caplog, batch_type, device):
    checkpoint = create_checkpoint()
    predictor = BatchPredictor.from_checkpoint(
        checkpoint, HuggingFacePredictor, task="text-generation"
    )

    # Todo: Ray data does not support numpy string arrays well
    if batch_type == np.ndarray:
        dataset = ray.data.from_numpy(prompts.to_numpy().astype("U"))
    elif batch_type == pd.DataFrame:
        dataset = ray.data.from_pandas(prompts)
    elif batch_type == pa.Table:
        dataset = ray.data.from_arrow(pa.Table.from_pandas(prompts))
    else:
        raise RuntimeError("Invalid batch_type")

    kwargs = {}
    if device:
        kwargs["device"] = device
    with caplog.at_level(logging.WARNING):
        predictions = predictor.predict(dataset, num_gpus_per_worker=1, **kwargs)
    assert "enable GPU prediction" not in caplog.text

    assert predictions.count() == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
