import datetime
import os
from typing import Any, Dict, Optional

import typer
import yaml
from rich import print
from rich.prompt import Confirm
from typing_extensions import Annotated

from ray.llm._internal.serve.config_generator.generator import (
    get_model_config,
    get_serve_config,
    override_model_configs,
)
from ray.llm._internal.serve.config_generator.inputs import (
    get_input_model_via_args,
    get_input_model_via_interactive_inputs,
)
from ray.llm._internal.serve.config_generator.utils.gpu import (
    GPUType,
    DEFAULT_MODEL_ID_TO_GPU,
)


def _format_yaml_dumper():
    """Changes the YAML dumper's format."""

    # Prompt formats often contain newline characters. We modify the YAML
    # dumper here, so it prints newline characters instead of actual newlines.
    def newline_representer(dumper, data):
        if "\n" in data:
            # Only adjust the dumper if a newline is present.
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')
        else:
            # Use the default dumper otherwise.
            return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    yaml.SafeDumper.add_representer(str, newline_representer)


def _write_model_config_to_disk(
    model_config: Dict[str, Any], model_id: str, timestamp: str
) -> str:
    # Set the filename with prefix, timestamp, and suffix
    sanitized_model_id = model_id.replace("/", "--").replace(".", "_")
    file_name = f"{sanitized_model_id}_{timestamp}.yaml"
    file_path = "./model_config"  # Directory where the file will be saved

    os.makedirs(file_path, exist_ok=True)

    # Create the full path for the file
    full_file_path = f"{file_path}/{file_name}"

    # Dump the configuration to the file
    with open(full_file_path, "w+") as file:
        yaml.safe_dump(model_config, file)
        return full_file_path


def _write_config_to_disk(serve_config: Dict[str, Any], timestamp: str):
    # Set the filename with prefix, timestamp, and suffix
    file_name = f"serve_{timestamp}.yaml"
    file_path = "./"  # Directory where the file will be saved

    # Create the full path for the file
    full_file_path = f"{file_path}{file_name}"

    # Dump the configuration to the file
    with open(full_file_path, "w+") as file:
        yaml.safe_dump(serve_config, file)

        return full_file_path


def _get_model_with_validation(
    model_id: Optional[str],
    hf_token: Optional[str],
    gpu_type: Optional[GPUType],
    tensor_parallelism: Optional[int],
    enable_lora: Optional[bool],
    num_loras_per_replica: Optional[int],
):
    if model_id and model_id not in DEFAULT_MODEL_ID_TO_GPU:
        raise RuntimeError(
            "In non-interactive CLI mode, please only specify model ID whose configs are provided by default."
        )

    if model_id and gpu_type and tensor_parallelism:
        return get_input_model_via_args(
            model_id,
            hf_token=hf_token,
            gpu_type=gpu_type,
            tensor_parallelism=tensor_parallelism,
            enable_lora=enable_lora,
            num_loras_per_replica=num_loras_per_replica,
        )
    else:
        raise RuntimeError(
            "If specifying args, please make sure all arguments are specified."
        )


def gen_config(
    model_id: Annotated[
        Optional[str],
        typer.Option(
            help="HuggingFace model ID to download and run",
            hidden=True,
        ),
    ] = None,
    hf_token: Annotated[
        Optional[str], typer.Option(help="Huggingface token", hidden=True)
    ] = None,
    gpu_type: Annotated[
        Optional[GPUType], typer.Option(help="Type of GPU", hidden=True)
    ] = None,
    tensor_parallelism: Annotated[
        Optional[int], typer.Option(help="Number of tensor parallelism", hidden=True)
    ] = None,
    enable_lora: Annotated[
        Optional[bool], typer.Option(help="Whether to enable LoRA serving", hidden=True)
    ] = False,
    # TODO (shrekris): Expose these hidden options after the API stabilizes.
    external_model_id: Annotated[
        Optional[str],
        typer.Option(
            help="[EXPERIMENTAL] Model ID to expose to end users. Defaults to model_id.",
            hidden=True,
        ),
    ] = None,
    override_lora_uri: Annotated[
        Optional[str],
        typer.Option(help="[EXPERIMENTAL] URI to LoRA weights", hidden=True),
    ] = None,
    num_loras_per_replica: Annotated[
        Optional[int],
        typer.Option(
            help="[EXPERIMENTAL] Number of LoRA weights per replica", hidden=True
        ),
    ] = None,
    enable_chunk_prefill: Annotated[
        Optional[bool],
        typer.Option(
            "--enable-chunked-prefill/--disable-chunked-prefill",
            help="Whether to enable chunked prefill",
            hidden=True,
        ),
    ] = None,
    max_num_batched_tokens: Annotated[
        Optional[int],
        typer.Option(help="Max Number of batched tokens", hidden=True),
    ] = None,
):
    """Starts an interactive session to generate RayLLM YAML configs."""

    if (override_lora_uri or num_loras_per_replica) and not enable_lora:
        raise ValueError(
            "--enable-lora is not set. --enable-lora must be set "
            "to use --override-lora-uri or --num-loras-per-replica."
        )

    if model_id or gpu_type or tensor_parallelism:
        interactive_mode = False
        input_model_config = _get_model_with_validation(
            model_id=model_id,
            hf_token=hf_token,
            gpu_type=gpu_type,
            tensor_parallelism=tensor_parallelism,
            enable_lora=enable_lora,
            num_loras_per_replica=num_loras_per_replica,
        )
    else:
        # sets up conditional vars for the interactive mode to not skip prompts
        interactive_mode = True
        input_model_config = get_input_model_via_interactive_inputs()

    model_config = get_model_config(input_model_config)
    if interactive_mode and Confirm.ask(
        "[bold]Further customize the auto-scaling config", default=False
    ):
        model_config = override_model_configs(model_config)

    if external_model_id:
        model_config["model_loading_config"]["model_id"] = external_model_id

    if override_lora_uri:
        model_config["lora_config"]["dynamic_lora_loading_path"] = override_lora_uri

    if enable_chunk_prefill is not None:
        model_config["engine_kwargs"]["enable_chunked_prefill"] = enable_chunk_prefill

    if max_num_batched_tokens is not None:
        model_config["engine_kwargs"]["max_num_batched_tokens"] = max_num_batched_tokens

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    _format_yaml_dumper()

    # Write the model config file to disk
    model_config_path = _write_model_config_to_disk(
        model_config, input_model_config.id, timestamp
    )
    serve_config = get_serve_config(model_config_path)

    full_file_path = _write_config_to_disk(serve_config, timestamp)

    print(
        f"\nYour serve configuration file is successfully written to {full_file_path}\n"
    )

    if input_model_config.id not in DEFAULT_MODEL_ID_TO_GPU:
        print(
            f"Note: we don't have serving defaults for {input_model_config.id} "
            "and we generally recommend you going over engine_kwargs in the yaml before serving. "
        )

    # Define the serve run command
    command_str = f"serve run {full_file_path}"
    print(f"Start the RayLLM app on this workspace by running: {command_str}")


if __name__ == "__main__":
    typer.run(gen_config)
