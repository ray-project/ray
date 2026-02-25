import os
from typing import Optional

from rich import print

from ray.llm._internal.serve.config_generator.utils.constants import HF_TOKEN_FILE
from ray.llm._internal.serve.config_generator.utils.prompt import BoldPrompt


def _read_persisted_hf_token() -> Optional[str]:
    if os.path.exists(HF_TOKEN_FILE):
        with open(HF_TOKEN_FILE, "r") as file:
            token = file.read().strip()
            if token:
                return token
            else:
                return None
    else:
        return None


def clear_hf_token():
    if os.path.isfile(HF_TOKEN_FILE):
        os.remove(HF_TOKEN_FILE)


def _write_hf_token(token: str):
    os.makedirs(os.path.dirname(HF_TOKEN_FILE), exist_ok=True)
    with open(HF_TOKEN_FILE, "w") as file:
        file.write(token)


_HF_TOKEN_PROMPT = "Please provide Hugging Face Access Token. Please get your token at https://huggingface.co/settings/tokens"


def get_hf_token():
    """
    Read from the local file if HF token is already persisted. If not, ask users for it.
    """
    stored_hf_token = _read_persisted_hf_token()

    if stored_hf_token:
        print(f"Your HuggingFace token is read from {HF_TOKEN_FILE}")
        return stored_hf_token
    else:
        hf_token = BoldPrompt.ask(_HF_TOKEN_PROMPT, default=None)
        _write_hf_token(hf_token)
        return hf_token
