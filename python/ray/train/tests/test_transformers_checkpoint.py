from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

from ray.train.huggingface import TransformersCheckpoint


from ray.train.tests.dummy_preprocessor import DummyPreprocessor
from ray.train.tests.test_torch_checkpoint import assert_equal_torch_models
from test_transformers_predictor import (
    model_checkpoint,
    tokenizer_checkpoint,
    test_strings,
)


def test_transformers_checkpoint(tmp_path):
    """This tests that TransformersCheckpoint created using `from_model` can
    be saved to a directory then loaded back with the same contents."""
    model_config = AutoConfig.from_pretrained(model_checkpoint)
    model = AutoModelForCausalLM.from_config(model_config)
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
    preprocessor = DummyPreprocessor()

    ckpt_save_path = tmp_path / "save_path"
    ckpt_path = tmp_path / "ckpt_dir"

    TransformersCheckpoint.from_model(
        model, tokenizer, path=str(ckpt_save_path), preprocessor=preprocessor
    ).to_directory(str(ckpt_path))

    checkpoint = TransformersCheckpoint.from_directory(str(ckpt_path))
    checkpoint_model = checkpoint.get_model(AutoModelForCausalLM)
    checkpoint_tokenizer = checkpoint.get_tokenizer(AutoTokenizer)
    checkpoint_preprocessor = checkpoint.get_preprocessor()

    tokens = tokenizer(test_strings)
    checkpoint_tokens = checkpoint_tokenizer(test_strings)

    assert_equal_torch_models(model, checkpoint_model)
    assert tokens == checkpoint_tokens
    assert checkpoint_preprocessor.id == preprocessor.id


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
