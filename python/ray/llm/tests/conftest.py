import os
import tempfile
import requests
import pytest


@pytest.fixture(scope="module")
def download_model_ckpt():
    """
    Download the model checkpoint and tokenizer from S3 for testing
    The reason to download the model from S3 is to avoid downloading the model
    from HuggingFace hub during testing, which is flaky because of the rate
    limit and HF hub downtime.
    """

    REMOTE_URL = (
        "https://air-example-data.s3.amazonaws.com/mistral-community-pixtral-12b/"
    )
    FILE_LIST = [
        "chat_template.json",
        "preprocessor_config.json",
        "processor_config.json",
        "special_tokens_map.json",
        "tokenizer_config.json",
        "tokenizer.json",
    ]

    # Create a temporary directory and save the model and tokenizer.
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        # Download the model checkpoint and tokenizer from S3.
        for file_name in FILE_LIST:
            response = requests.get(REMOTE_URL + file_name)
            with open(os.path.join(checkpoint_dir, file_name), "wb") as fp:
                fp.write(response.content)

        yield os.path.abspath(checkpoint_dir)
