import os
import tempfile
import requests
import pytest
from typing import Generator, List

S3_ARTIFACT_URL = "https://air-example-data.s3.amazonaws.com/"


def download_model_from_s3(
    remote_url: str, file_list: List[str]
) -> Generator[str, None, None]:
    """
    Download the model checkpoint and tokenizer from S3 for testing
    The reason to download the model from S3 is to avoid downloading the model
    from HuggingFace hub during testing, which is flaky because of the rate
    limit and HF hub downtime.

    Args:
        remote_url: The remote URL to download the model from.
        file_list: The list of files to download.

    Yields:
        The path to the downloaded model checkpoint and tokenizer.
    """
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        for file_name in file_list:
            response = requests.get(remote_url + file_name)
            with open(os.path.join(checkpoint_dir, file_name), "wb") as fp:
                fp.write(response.content)
        yield os.path.abspath(checkpoint_dir)


@pytest.fixture(scope="session")
def model_opt_125m():
    """The small decoder model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}facebook-opt-125m/"
    FILE_LIST = [
        "config.json",
        "flax_model.msgpack",
        "generation_config.json",
        "merges.txt",
        "pytorch_model.bin",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "vocab.json",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def model_llava_1_5_7b():
    """The vision language model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}llava-hf-llava-1.5-7b-hf/"
    FILE_LIST = [
        "config.json",
        "chat_template.json",
        "added_tokens.json",
        "generation_config.json",
        "model-00001-of-00003.safetensors",
        "model-00002-of-00003.safetensors",
        "model-00003-of-00003.safetensors",
        "model.safetensors.index.json",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer.model",
        "tokenizer_config.json",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)
