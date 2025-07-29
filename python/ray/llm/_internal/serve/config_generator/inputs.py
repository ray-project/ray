from typing import List, Optional

from huggingface_hub import repo_info
from pydantic import BaseModel
from rich import print
from rich.prompt import Confirm, Prompt

from ray.llm._internal.serve.config_generator.utils.constants import (
    get_lora_storage_uri,
)
from ray.llm._internal.serve.config_generator.utils.files import (
    clear_hf_token,
    get_hf_token,
)
from ray.llm._internal.serve.config_generator.utils.gpu import (
    DEFAULT_MODEL_ID_TO_GPU,
    GPUType,
    get_available_gpu_types,
)
from ray.llm._internal.serve.config_generator.utils.input_converter import (
    convert_inputs_to_text_completion_model,
)
from ray.llm._internal.serve.config_generator.utils.models import (
    TEXT_COMPLETION_MODEL_TYPE,
    ModelType,
    TextCompletionLoraModelConfig,
    TextCompletionModelConfig,
)
from ray.llm._internal.serve.config_generator.utils.prompt import (
    BoldIntPrompt,
    BoldPrompt,
)
from ray.llm._internal.serve.config_generator.utils.text_completion import (
    get_default_llm_config,
)


def _get_user_input_from_list(prompt: str, options: List[GPUType]):
    while True:
        user_input = BoldPrompt.ask(
            f"{prompt}",
            choices=[option.value for option in options],
        )
        for option in options:
            if user_input.lower() == option.value.lower():
                return option
        print(f"Please enter a valid option from {options}")


class BaseModelInfo(BaseModel):
    id: str
    hf_token: Optional[str] = None
    type: ModelType
    import_model_storage_uri: Optional[str] = None
    reference_model_id: Optional[str] = None


def _get_user_input_from_lists(
    prompt: str, options: List[str], allow_any_user_input: bool
):
    while True:
        res = Prompt.ask(prompt)
        if allow_any_user_input or res in options:
            return res
        else:
            print("Please enter a valid option.")


def _get_model_id():
    model_list = list(DEFAULT_MODEL_ID_TO_GPU.keys())
    model_ids = "\n".join(model_list)
    prompt_message = f"""[bold]We have provided the defaults for the following models[not bold]:
{model_ids}
[bold]Please enter the model ID you would like to serve, or enter your own custom model ID. For multi-lora serving, enter only the base model ID"""

    model_id = _get_user_input_from_lists(
        prompt_message, model_list, allow_any_user_input=True
    )
    return model_id


def _get_validated_model_info():
    """
    If the model is supposed to be loaded from huggingface, we make an API call to confirm that
    user's credentials work.
    """
    while True:
        ref_model_id = None

        model_id = _get_model_id()
        model_type = TEXT_COMPLETION_MODEL_TYPE
        if model_id not in DEFAULT_MODEL_ID_TO_GPU:
            ref_model_id = "other"
            import_model = Confirm.ask(
                "Do you have the model weights stored in your cloud buckets? "
                "If not, model weights will be downloaded from HuggingFace.",
                default=False,
            )

            if import_model:
                # No need to validate since the model is loaded from remote uri
                remote_storage_uri = BoldPrompt.ask("Storage uri of your model: ")
                return BaseModelInfo(
                    id=model_id,
                    hf_token=None,
                    type=model_type,
                    import_model_storage_uri=remote_storage_uri,
                    reference_model_id=ref_model_id,
                )

        hf_token = get_hf_token()

        # Need to validate
        try:
            repo_info(model_id, token=hf_token)
            print(
                f"Verified that you can access {model_id} with your HuggingFace token."
            )
            return BaseModelInfo(
                id=model_id,
                hf_token=hf_token,
                type=model_type,
                reference_model_id=ref_model_id,
            )
        except Exception as e:
            print(f"An error occurred: {e}")
            # Clear cached tf token in case it was incorrectly entered
            clear_hf_token()

            print(
                "\nUnable to access the HuggingFace repo based on the "
                "provided information. Please confirm that you have access "
                "to the model and try again."
            )


def _get_default_tensor_parallelism(model_id: str, gpu_type: GPUType) -> int:
    if model_id in DEFAULT_MODEL_ID_TO_GPU:
        llm_config = get_default_llm_config(model_id=model_id, gpu_type=gpu_type)
        return llm_config.engine_kwargs.setdefault("tensor_parallel_size", 1)

    return 1


_DEFAULT_NUM_LORA_PER_REPLICA = 16


def get_input_model_via_args(
    model_id: str,
    hf_token: Optional[str],
    gpu_type: GPUType,
    tensor_parallelism: int,
    enable_lora: Optional[bool],
    num_loras_per_replica: Optional[int],
) -> TextCompletionModelConfig:
    if enable_lora:
        max_num_lora_per_replica = (
            num_loras_per_replica or _DEFAULT_NUM_LORA_PER_REPLICA
        )
        default_lora_uri = get_lora_storage_uri()
        lora_config = TextCompletionLoraModelConfig(
            max_num_lora_per_replica=max_num_lora_per_replica,
            uri=default_lora_uri,
        )
    else:
        lora_config = None

    return convert_inputs_to_text_completion_model(
        model_id=model_id,
        hf_token=hf_token,
        gpu_type=gpu_type,
        tensor_parallelism=tensor_parallelism,
        lora_config=lora_config,
    )


def get_input_model_via_interactive_inputs() -> TextCompletionModelConfig:
    """
    This method constructs a corresponding model object based on user inputs.

    This model object should be used to construct the final serve config.
    """
    base_model_info = _get_validated_model_info()

    available_gpu_types = get_available_gpu_types(base_model_info.id)
    gpu_type = _get_user_input_from_list("GPU type", available_gpu_types)

    default_tensor_parallelism = _get_default_tensor_parallelism(
        base_model_info.id, gpu_type=gpu_type
    )

    tensor_parallelism = BoldIntPrompt.ask(
        "Tensor parallelism", default=default_tensor_parallelism
    )

    is_lora_enabled = Confirm.ask("[bold]Enable LoRA serving", default=False)

    if is_lora_enabled:
        default_lora_uri = get_lora_storage_uri()
        max_num_lora_per_replica = BoldIntPrompt.ask(
            "Maximum number of LoRA models per replica",
            default=_DEFAULT_NUM_LORA_PER_REPLICA,
        )
        lora_uri = BoldPrompt.ask("LoRA storage uri", default=default_lora_uri)
        lora_config = TextCompletionLoraModelConfig(
            max_num_lora_per_replica=max_num_lora_per_replica,
            uri=lora_uri,
        )
    else:
        lora_config = None

    return convert_inputs_to_text_completion_model(
        model_id=base_model_info.id,
        hf_token=base_model_info.hf_token,
        remote_storage_uri=base_model_info.import_model_storage_uri,
        gpu_type=gpu_type,
        tensor_parallelism=tensor_parallelism,
        lora_config=lora_config,
        reference_model_id=base_model_info.reference_model_id,
    )
