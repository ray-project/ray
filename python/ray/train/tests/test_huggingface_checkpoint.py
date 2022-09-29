import os
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
from transformers.pipelines import pipeline

import ray
from ray.train.huggingface import HuggingFaceCheckpoint, HuggingFacePredictor


from dummy_preprocessor import DummyPreprocessor
from test_huggingface_predictor import (
    model_checkpoint,
    tokenizer_checkpoint,
    test_strings,
    prompts,
)


def test_huggingface_checkpoint(tmpdir, ray_start_runtime_env):
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
    preprocessor = DummyPreprocessor()

    checkpoint = HuggingFaceCheckpoint.from_model(
        model, tokenizer, path=tmpdir, preprocessor=preprocessor
    )
    checkpoint_model = checkpoint.get_model(AutoModelForCausalLM)
    checkpoint_tokenizer = checkpoint.get_tokenizer(AutoTokenizer)
    checkpoint_preprocessor = checkpoint.get_preprocessor()

    @ray.remote
    def test(model, tokenizer, preprocessor):
        os.chdir(tmpdir)
        predictor = HuggingFacePredictor(
            pipeline=pipeline(
                task="text-generation",
                model=model,
                tokenizer=tokenizer,
            ),
            preprocessor=preprocessor,
        )

        predictions = predictor.predict(prompts)
        if preprocessor:
            assert predictor.get_preprocessor().has_preprocessed
        return predictions

    tokens = tokenizer(test_strings)
    checkpoint_tokens = checkpoint_tokenizer(test_strings)

    predictions = ray.get(test.remote(model, tokenizer, preprocessor))
    checkpoint_predictions = ray.get(
        test.remote(checkpoint_model, checkpoint_tokenizer, checkpoint_preprocessor)
    )

    assert all(predictions == checkpoint_predictions)
    assert tokens == checkpoint_tokens
    assert checkpoint_preprocessor == preprocessor


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
