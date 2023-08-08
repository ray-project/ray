import tempfile

import pandas as pd
import pytest
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer


from ray.train.huggingface import (
    TransformersCheckpoint,
    TransformersPredictor,
)

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
        checkpoint = TransformersCheckpoint.from_model(model, tokenizer, path=tmpdir)
        # Serialize to dict so we can remove the temporary directory
        return TransformersCheckpoint.from_dict(checkpoint.to_dict())


class AssertingTransformersPredictor(TransformersPredictor):
    def __init__(self, pipeline=None, preprocessor=None, use_gpu: bool = False):
        super().__init__(pipeline, preprocessor, use_gpu)
        assert use_gpu
        assert "cuda" in str(pipeline.device)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
