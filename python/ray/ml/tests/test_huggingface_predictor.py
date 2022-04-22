import pandas as pd
import pytest
from transformers import AutoConfig, AutoModelForCausalLM, TrainingArguments

from ray.ml.preprocessor import Preprocessor
from ray.ml.predictors.integrations.huggingface import HuggingFacePredictor

from ray.ml.tests._huggingface_data import validation_data

# 16 first rows of tokenized wikitext-2-raw-v1 validation
validation_df = pd.read_json(validation_data)

# We are only testing Casual Language Modelling here

model_checkpoint = "sshleifer/tiny-gpt2"


class DummyPreprocessor(Preprocessor):
    def transform_batch(self, df):
        self._batch_transformed = True
        return df


@pytest.mark.parametrize("preprocessor", [True, False])
@pytest.mark.parametrize("training_args", [True, False])
def test_predict(preprocessor, training_args, tmpdir):
    if preprocessor:
        preprocessor = DummyPreprocessor()
    else:
        preprocessor = None
    if training_args:
        training_args = TrainingArguments(tmpdir)
    else:
        training_args = None
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    predictor = HuggingFacePredictor(
        model=model, preprocessor=preprocessor, training_args=training_args
    )

    predictions = predictor.predict(validation_df)

    assert len(predictions) == 16
    if preprocessor:
        assert hasattr(predictor.preprocessor, "_batch_transformed")
