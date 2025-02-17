import os
import tempfile
import requests
import pytest


CHECKPOINT_FILES = {
    "facebook-opt-125m": [
        "config.json",
        "flax_model.msgpack",
        "generation_config.json",
        "merges.txt",
        "pytorch_model.bin",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "vocab.json",
    ],
    "mistral-community-pixtral-12b": [
        "config.json",
        "chat_template.json",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
    ],
    "meta-llama-llama2-7b": [
        "config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
        "tokenizer.model",
    ],
}


@pytest.fixture
def download_model_ckpt_model():
    return "facebook-opt-125m"


@pytest.fixture
def download_model_ckpt(download_model_ckpt_model):
    """
    Download the model checkpoint and tokenizer from S3 for testing
    The reason to download the model from S3 is to avoid downloading the model
    from HuggingFace hub during testing, which is flaky because of the rate
    limit and HF hub downtime.
    """
    model = download_model_ckpt_model
    if model not in CHECKPOINT_FILES:
        print(
            f'Model {model} not found in the list of available models. Use "facebook-opt-125m" as default model.'
        )
        model = "facebook-opt-125m"

    print(f"Downloading model {download_model_ckpt_model} from S3 for testing.")
    remote_url = f"https://air-example-data.s3.amazonaws.com/{model}/"
    file_list = CHECKPOINT_FILES[model]

    # Create a temporary directory and save the model and tokenizer.
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        # Download the model checkpoint and tokenizer from S3.
        for file_name in file_list:
            response = requests.get(remote_url + file_name)
            with open(os.path.join(checkpoint_dir, file_name), "wb") as fp:
                fp.write(response.content)

        yield os.path.abspath(checkpoint_dir)
