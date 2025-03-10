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
    with tempfile.TemporaryDirectory(prefix="ray-llm-test-model") as checkpoint_dir:
        print(f"Downloading model from {remote_url} to {checkpoint_dir}", flush=True)
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
def model_llava_354m():
    """The vision language model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}llava-354M/"
    FILE_LIST = [
        "added_tokens.json",
        "chat_template.json",
        "config.json",
        "generation_config.json",
        "model.safetensors",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer.model",
        "tokenizer_config.json",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def model_llama_3_2_216M():
    """The llama 3.2 216M model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}llama-3.2-216M-dummy/"
    FILE_LIST = [
        "config.json",
        "generation_config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
        "model.safetensors",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def model_llama_3_2_216M_lora():
    """The LoRA model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}llama-3.2-216M-lora-dummy/"
    FILE_LIST = [
        "adapter_config.json",
        "adapter_model.safetensors",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def model_pixtral_12b():
    """The Pixtral 12B model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}mistral-community-pixtral-12b/"
    FILE_LIST = [
        "config.json",
        "chat_template.json",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def model_llama_3_2_1B_instruct():
    """The llama 3.2 1B Instruct model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_URL}unsloth-Llama-3.2-1B-Instruct/"
    FILE_LIST = [
        "config.json",
        "generation_config.json",
        "model.safetensors",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
    ]
    yield from download_model_from_s3(REMOTE_URL, FILE_LIST)


@pytest.fixture(scope="session")
def gpu_type():
    """Get the GPU type used for testing."""

    try:
        import torch

        print(f"{torch.version.cuda=}", flush=True)
        name = torch.cuda.get_device_name()
        # The name of the GPU is in the format of "NVIDIA L4" or "Tesla T4".
        _, type_name = name.split(" ")
        print(f"GPU type: {type_name}", flush=True)
        yield type_name
    except ImportError:
        print("Failed to import torch to get GPU type", flush=True)
    except ValueError as err:
        print(f"Failed to get the GPU type: {err}", flush=True)
