import os

import pandas as pd
import pytest
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
from transformers.pipelines import pipeline

import ray
from ray.data.preprocessor import Preprocessor
from ray.train.huggingface import HuggingFacePredictor

prompts = pd.DataFrame(
    ["Complete me", "And me", "Please complete"], columns=["sentences"]
)

# We are only testing Casual Language Modeling here

model_checkpoint = "sshleifer/tiny-gpt2"
tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"


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


def test_predict(tmpdir, ray_start_runtime_env):
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

        predictions = predictor.predict(prompts)

        assert len(predictions) == 3
        if preprocessor:
            assert hasattr(predictor.preprocessor, "_batch_transformed")

    ray.get(test.remote(use_preprocessor=True))
    ray.get(test.remote(use_preprocessor=False))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
