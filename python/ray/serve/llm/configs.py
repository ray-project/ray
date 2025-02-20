from ray.llm._internal.serve.configs.server_models import (
    LLMConfig as _LLMConfig,
    LLMServingArgs as _LLMServingArgs,
    ModelLoadingConfig as _ModelLoadingConfig,
    S3AWSCredentials as _S3AWSCredentials,
    S3MirrorConfig as _S3MirrorConfig,
    GCSMirrorConfig as _GCSMirrorConfig,
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


# TODO (Kourosh): S3AWSCredentials should be removed. It feels extra.
@PublicAPI(stability="alpha")
class S3AWSCredentials(_S3AWSCredentials):
    """The configuration for loading an LLM model from S3."""

    pass


@PublicAPI(stability="alpha")
class S3MirrorConfig(_S3MirrorConfig):
    """The configuration for mirroring an LLM model from S3."""

    pass


@PublicAPI(stability="alpha")
class GCSMirrorConfig(_GCSMirrorConfig):
    """The configuration for mirroring an LLM model from GCS."""

    pass


@PublicAPI(stability="alpha")
class LoraConfig(_LoraConfig):
    """The configuration for loading an LLM model with LoRA."""

    pass
