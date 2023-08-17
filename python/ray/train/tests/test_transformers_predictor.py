import os
import re
import tempfile

import numpy as np
import pandas as pd
import pytest
from ray.air.constants import MAX_REPR_LENGTH
from ray.air.util.data_batch_conversion import _convert_pandas_to_batch_type
from ray.train.predictor import TYPE_TO_ENUM
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
    GPT2LMHeadModel,
)
from transformers.pipelines import pipeline, Pipeline


from ray.train.huggingface import (
    LegacyTransformersCheckpoint,
    TransformersPredictor,
)

from ray.train.tests.dummy_preprocessor import DummyPreprocessor

test_strings = ["Complete me", "And me", "Please complete"]
prompts = pd.DataFrame(test_strings, columns=["sentences"])

# We are only testing Causal Language Modeling here

model_checkpoint = "hf-internal-testing/tiny-random-gpt2"
tokenizer_checkpoint = "hf-internal-testing/tiny-random-gpt2"


class CustomPipeline(Pipeline):
    def _forward(self, input_tensors, **forward_parameters):
        pass

    def _sanitize_parameters(self, **pipeline_parameters):
        return {}, {}, {}

    def postprocess(self, model_outputs, **postprocess_parameters):
        pass

    def preprocess(self, input_, **preprocess_parameters):
        pass


def test_repr(tmpdir):
    predictor = TransformersPredictor()

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^TransformersPredictor\\((.*)\\)$")
    assert pattern.match(representation)


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, dict])
def test_predict(tmpdir, ray_start_4_cpus, batch_type):
    dtype_prompts = _convert_pandas_to_batch_type(
        prompts, type=TYPE_TO_ENUM[batch_type]
    )

    os.chdir(tmpdir)

    def test(use_preprocessor):
        if use_preprocessor:
            preprocessor = DummyPreprocessor()
        else:
            preprocessor = None
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        predictor = TransformersPredictor(
            pipeline=pipeline(
                task="text-generation",
                model=model,
                tokenizer=AutoTokenizer.from_pretrained(tokenizer_checkpoint),
            ),
            preprocessor=preprocessor,
        )

        predictions = predictor.predict(dtype_prompts)

        assert len(predictions) == 3
        if preprocessor:
            assert predictor.get_preprocessor().has_preprocessed

    test(use_preprocessor=True)
    test(use_preprocessor=False)


def test_predict_no_preprocessor_no_training(tmpdir, ray_start_4_cpus):
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
    checkpoint = LegacyTransformersCheckpoint.from_model(model, tokenizer, path=tmpdir)
    predictor = TransformersPredictor.from_checkpoint(
        checkpoint,
        task="text-generation",
    )

    predictions = predictor.predict(prompts)

    assert len(predictions) == 3


@pytest.mark.parametrize("model_cls", [GPT2LMHeadModel, None])
def test_custom_pipeline(tmpdir, model_cls):
    """Create predictor from a custom pipeline class."""
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
    checkpoint = LegacyTransformersCheckpoint.from_model(model, tokenizer, path=tmpdir)

    if model_cls:
        kwargs = {}
    else:
        kwargs = {"task": "text-generation"}

    predictor = TransformersPredictor.from_checkpoint(
        checkpoint, pipeline_cls=CustomPipeline, model_cls=model_cls, **kwargs
    )
    assert isinstance(predictor.pipeline, CustomPipeline)


def create_checkpoint():
    with tempfile.TemporaryDirectory() as tmpdir:
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
        checkpoint = LegacyTransformersCheckpoint.from_model(
            model, tokenizer, path=tmpdir
        )
        # Serialize to dict so we can remove the temporary directory
        return LegacyTransformersCheckpoint.from_dict(checkpoint.to_dict())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
