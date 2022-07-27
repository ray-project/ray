import os
import re
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from ray.air.constants import MAX_REPR_LENGTH
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import TYPE_TO_ENUM
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
from transformers.pipelines import pipeline

import ray
from ray.data.preprocessor import Preprocessor
from ray.train.huggingface import HuggingFaceCheckpoint, HuggingFacePredictor

prompts = pd.DataFrame(
    ["Complete me", "And me", "Please complete"], columns=["sentences"]
)

# We are only testing Casual Language Modeling here

model_checkpoint = "sshleifer/tiny-gpt2"
tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_runtime_env():
    # Requires at least torch 1.11 to pass
    # TODO update torch version in requirements instead
    runtime_env = {"pip": ["torch==1.11.0"]}
    address_info = ray.init(runtime_env=runtime_env)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df


def test_repr(tmpdir):
    predictor = HuggingFacePredictor()

    representation = repr(predictor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^HuggingFacePredictor\\((.*)\\)$")
    assert pattern.match(representation)


@pytest.mark.parametrize("batch_type", [np.ndarray, pd.DataFrame, pa.Table, dict])
def test_predict(tmpdir, ray_start_runtime_env, batch_type):
    dtype_prompts = convert_pandas_to_batch_type(prompts, type=TYPE_TO_ENUM[batch_type])

    @ray.remote
    def test(use_preprocessor):
        os.chdir(tmpdir)
        if use_preprocessor:
            preprocessor = DummyPreprocessor()
        else:
            preprocessor = None
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        predictor = HuggingFacePredictor(
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
            assert hasattr(predictor.get_preprocessor(), "_batch_transformed")

    ray.get(test.remote(use_preprocessor=True))
    ray.get(test.remote(use_preprocessor=False))


def test_predict_no_preprocessor_no_training(ray_start_runtime_env):
    @ray.remote
    def test():
        with tempfile.TemporaryDirectory() as tmpdir:
            model_config = AutoConfig.from_pretrained(model_checkpoint)
            model = AutoModelForCausalLM.from_config(model_config)
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
            checkpoint = HuggingFaceCheckpoint.from_model(model, tokenizer, path=tmpdir)
            predictor = HuggingFacePredictor.from_checkpoint(
                checkpoint,
                task="text-generation",
            )

            predictions = predictor.predict(prompts)

            assert len(predictions) == 3

    ray.get(test.remote())


def create_checkpoint():
    with tempfile.TemporaryDirectory() as tmpdir:
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
        checkpoint = HuggingFaceCheckpoint.from_model(model, tokenizer, path=tmpdir)
        # Serialize to dict so we can remove the temporary directory
        return HuggingFaceCheckpoint.from_dict(checkpoint.to_dict())


@pytest.mark.parametrize("batch_type", [pd.DataFrame, pa.Table])
def test_predict_batch(ray_start_4_cpus, batch_type):
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

    predictions = predictor.predict(dataset)

    assert predictions.count() == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
