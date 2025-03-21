import random
from functools import cache
from typing import TYPE_CHECKING

import probes.config as config
from probes.openai_client import openai_client

if TYPE_CHECKING:
    import openai


def ids(models):
    return [model.id for model in models]


# These models are used in release tests.
RELEASE_TEST_MODELS = [
    # Fine tuned version of Meta Llama-3 8b.
    "meta-llama/Meta-Llama-3.1-8B-Instruct-Fine-Tuned",
]


class ModelLoader:
    def __init__(self, models=None):
        self.models = models or load_models()

    def model_ids(self):
        return (
            self.base_model_ids()
            + self.finetune_model_ids()
            + self.completions_only_model_ids()
        )

    def base_models(self):
        return [m for m in self.models if not is_finetuned_model(m)]

    def completions_only_models(self):
        return [m for m in self.models if is_completions_only_model(m)]

    def base_model_ids(self):
        return ids(self.base_models())

    def completions_only_model_ids(self):
        return ids(self.completions_only_models())

    def finetuned_models(self):
        return [m for m in self.models if is_finetuned_model(m)]

    def finetune_model_ids(self):
        return ids(self.finetuned_models())

    def json_mode_models(self):
        """These are models that have constrained generation enabled"""
        return [m for m in self.models if supports_json_mode(m)]

    def json_mode_model_ids(self):
        return ids(self.json_mode_models())

    def function_calling_models(self):
        """These are models that natively support function calling via their prompt"""
        return [m for m in self.models if supports_function_calling_via_prompt(m)]

    def function_calling_model_ids(self):
        return [m.id for m in self.function_calling_models()]

    def rate_limiting_model_ids(self):
        return [m.id for m in self.models if is_rate_liming_test_model(m)]

    def vision_language_models(self):
        return [m for m in self.models if is_vision_language_model(m)]

    def vision_language_model_ids(self):
        return [m.id for m in self.models if is_vision_language_model(m)]

    def long_context_models(self):
        return [m for m in self.models if m.id in config.get("long_context_models")]

    def long_context_model_ids(self):
        return [m.id for m in self.long_context_models()]

    def base_llama_models(self):
        return [m for m in self.models if "llama" in m.id and not is_finetuned_model(m)]

    def llama_model_ids(self):
        return ids(self.base_llama_models())

    def speculative_decoding_model_ids(self):
        return [m.id for m in self.models if is_speculative_decoding_model(m)]

    def release_test_model_ids(self):
        return [m.id for m in self.models if is_release_test_model(m)]


def is_release_test_model(model):
    return model.id in RELEASE_TEST_MODELS


def is_finetuned_model(model):
    # If base_model_id is set, this is a finetuned model
    return model.rayllm_metadata.get("base_model_id") is not None


def is_vision_language_model(model: "openai.types.model.Model"):
    return model.rayllm_metadata.get("input_modality") == "image"


def is_rate_liming_test_model(model):
    model_id = model if isinstance(model, str) else model.id
    return model_id in config.get("rate_limiting_models")


def is_vision_language_model_id(model_id: str):
    return model_id in model_loader.vision_language_model_ids()


def supports_json_mode(model):
    """All models should now support JSON mode"""
    return True


def is_speculative_decoding_model(model):
    model_id = model if isinstance(model, str) else model.id
    return model_id in set(config.get("speculative_decoding_models"))


def is_completions_only_model(model):
    model_id = model if isinstance(model, str) else model.id
    return model_id in config.get("completions_only_models")


def supports_function_calling_via_prompt(model):
    # True if tool template is specified in the generation config
    gen_config = model.rayllm_metadata.get("generation", False)

    if not gen_config:
        return False

    prompt_format = gen_config["prompt_format"]
    return bool(prompt_format.get("tool", ""))


@cache
def load_models():
    return [
        m
        for m in openai_client.models.list().data
        if m.id not in config.get("ignored_models", [])
    ]


model_loader = ModelLoader()
random_model = random.choice(model_loader.model_ids())
