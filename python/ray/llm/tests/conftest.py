import base64
import io
import os
import tempfile
from typing import Generator, List

import PIL.Image
import pytest
import requests

from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_llm_deployment

S3_ARTIFACT_URL = "https://air-example-data.s3.amazonaws.com/"
S3_ARTIFACT_LLM_OSSCI_URL = S3_ARTIFACT_URL + "rayllm-ossci/"


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
def model_smolvlm_256m():
    """The vision language model for testing."""
    REMOTE_URL = f"{S3_ARTIFACT_LLM_OSSCI_URL}smolvlm-256m-instruct/"
    FILE_LIST = [
        "added_tokens.json",
        "chat_template.json",
        "config.json",
        "generation_config.json",
        "merges.txt",
        "model.safetensors",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "vocab.json",
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
def model_qwen_2_5_vl_3b_instruct():
    # REMOTE_URL = f"{S3_ARTIFACT_URL}Qwen2.5-VL-3B-Instruct/"
    # FILE_LIST = [
    #     "config.json",
    #     "chat_template.json",
    #     "generation_config.json",
    #     "merges.txt",
    #     "model-00001-of-00002.safetensors",
    #     "model-00002-of-00002.safetensors"
    #     "model.safetensors.index.json",
    #     "preprocessor_config.json",
    #     "tokenizer.json",
    #     "tokenizer_config.json",
    #     "vocab.json",
    # ]
    # yield from download_model_from_s3(REMOTE_URL, FILE_LIST)
    yield "Qwen/Qwen2.5-VL-3B-Instruct"


@pytest.fixture(scope="session")
def model_qwen_2_5_omni_3b():
    # REMOTE_URL = f"{S3_ARTIFACT_URL}Qwen2.5-Omni-3B/"
    # FILE_LIST = [
    #     "added_tokens.json",
    #     "config.json",
    #     "chat_template.json",
    #     "generation_config.json",
    #     "merges.txt",
    #     "model-00001-of-00003.safetensors",
    #     "model-00002-of-00003.safetensors",
    #     "model-00003-of-00003.safetensors",
    #     "model.safetensors.index.json",
    #     "preprocessor_config.json",
    #     "special_tokens_map.json",
    #     "spk_dict.json",
    #     "tokenizer.json",
    #     "tokenizer_config.json",
    #     "vocab.json",
    # ]
    # yield from download_model_from_s3(REMOTE_URL, FILE_LIST)
    yield "Qwen/Qwen2.5-Omni-3B"


@pytest.fixture(scope="session")
def gpu_type():
    """Get the GPU type used for testing."""

    try:
        import torch

        print(f"{torch.version.cuda=}", flush=True)
        name = torch.cuda.get_device_name()
        # The name of the GPU is in the format of "NVIDIA L4" or "Tesla T4"
        # or "NVIDIA H100 80GB HBM3"
        type_name = name.split(" ")[1]
        print(f"GPU type: {type_name}", flush=True)
        yield type_name
    except ImportError:
        print("Failed to import torch to get GPU type", flush=True)
    except ValueError as err:
        print(f"Failed to get the GPU type: {err}", flush=True)


@pytest.fixture
def serve_cleanup():
    yield
    serve.shutdown()


@pytest.fixture
def create_model_opt_125m_deployment(gpu_type, model_opt_125m, serve_cleanup):
    """Create a serve deployment for testing."""
    app_name = "test_serve_deployment_processor_app"
    deployment_name = "test_deployment_name"

    chat_template = """
{% if messages[0]['role'] == 'system' %}
    {% set offset = 1 %}
{% else %}
    {% set offset = 0 %}
{% endif %}

{{ bos_token }}
{% for message in messages %}
    {% if (message['role'] == 'user') != (loop.index0 % 2 == offset) %}
        {{ raise_exception('Conversation roles must alternate user/assistant/user/assistant/...') }}
    {% endif %}

    {{ '<|im_start|>' + message['role'] + '\n' + message['content'] | trim + '<|im_end|>\n' }}
{% endfor %}

{% if add_generation_prompt %}
    {{ '<|im_start|>assistant\n' }}
{% endif %}
    """

    # Create a vLLM serve deployment
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_opt_125m,
            model_source=model_opt_125m,
        ),
        accelerator_type=gpu_type,
        deployment_config=dict(
            name="test_deployment_name",  # This is not necessarily the final deployment name
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            ),
        ),
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            # Add chat template for OPT model to enable chat API
            chat_template=chat_template,
        ),
    )

    llm_app = build_llm_deployment(
        llm_config, override_serve_options=dict(name=deployment_name)
    )
    serve.run(llm_app, name=app_name)
    yield deployment_name, app_name


@pytest.fixture
def image_asset():
    image_url = "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
    with requests.get(image_url) as response:
        response.raise_for_status()
        image_pil = PIL.Image.open(io.BytesIO(response.content))
        yield image_url, image_pil


@pytest.fixture
def audio_asset():
    audio_url = "https://vllm-public-assets.s3.us-west-2.amazonaws.com/multimodal_asset/winning_call.ogg"
    with requests.get(audio_url) as response:
        response.raise_for_status()
        audio_data = base64.b64encode(response.content).decode("utf-8")
        yield audio_url, audio_data
