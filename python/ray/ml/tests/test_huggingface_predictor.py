import pandas as pd
import pytest

from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoTokenizer,
)
from transformers.pipelines import pipeline

from ray.ml.preprocessor import Preprocessor
from ray.ml.predictors.integrations.huggingface import HuggingFacePredictor

prompts = pd.DataFrame(
    ["Complete me", "And me", "Please complete"], columns=["sentences"]
)

# We are only testing Casual Language Modeling here

model_checkpoint = "sshleifer/tiny-gpt2"
tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df


@pytest.mark.parametrize("preprocessor", [True, False])
def test_predict(preprocessor, tmpdir):
    if preprocessor:
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
