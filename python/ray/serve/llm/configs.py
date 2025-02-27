from ray.llm._internal.serve.configs.server_models import (
    LLMConfig as _LLMConfig,
    LLMServingArgs as _LLMServingArgs,
    ModelLoadingConfig as _ModelLoadingConfig,
    CloudMirrorConfig as _CloudMirrorConfig,
    LoraConfig as _LoraConfig,
)

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LLMConfig(_LLMConfig):
    """The configuration for starting an LLM deployment."""

    pass


@PublicAPI(stability="alpha")
class LLMServingArgs(_LLMServingArgs):
    """The configuration for starting an LLM deployment application."""

    pass


@PublicAPI(stability="alpha")
class ModelLoadingConfig(_ModelLoadingConfig):
    """The configuration for loading an LLM model."""

    pass


@PublicAPI(stability="alpha")
class CloudMirrorConfig(_CloudMirrorConfig):
    """The configuration for mirroring an LLM model from cloud storage."""

    pass


@PublicAPI(stability="alpha")
class LoraConfig(_LoraConfig):
    """The configuration for loading an LLM model with LoRA."""

    pass
