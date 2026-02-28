from ray.llm._internal.common.callbacks.base import (
    CallbackBase as _CallbackBase,
    CallbackConfig as _CallbackConfig,
    CallbackCtx as _CallbackCtx,
)
from ray.llm._internal.common.callbacks.cloud_downloader import (
    CloudDownloader as _CloudDownloader,
)
from ray.util.annotations import PublicAPI

##########
# Callbacks
##########


@PublicAPI(stability="alpha")
class CallbackCtx(_CallbackCtx):
    """Context object passed to all callback hooks during LLM deployment initialization."""

    pass


@PublicAPI(stability="alpha")
class CallbackBase(_CallbackBase):
    """Base class for custom initialization callbacks in Ray Serve LLM deployments.

    Subclass ``CallbackBase`` to inject custom logic at key points during LLM
    deployment initialization. Callbacks are configured via ``CallbackConfig``
    and attached to ``LLMConfig`` through the ``callback_config`` field.

    Lifecycle hooks (called in order during deployment startup):

    - ``on_before_node_init``: Runs in the Serve replica process before
      default model download. Use to download from custom storage or skip
      default init via ``self.ctx.run_init_node = False``.
    - ``on_after_node_init``: Runs in the Serve replica process after
      default model download. Use to register reasoning parsers, tokenizers,
      or plugins.
    - ``on_before_download_model_files_distributed``: Runs as a remote task
      on each node in the placement group before it downloads model files.
      Use to download auxiliary files.

    Examples:
        .. testcode::
            :skipif: True

            from ray.serve.llm import LLMConfig
            from ray.serve.llm.callbacks import CallbackBase

            class ReasoningParserCallback(CallbackBase):
                async def on_after_node_init(self) -> None:
                    from vllm.reasoning import ReasoningParserManager
                    parser_name = self.kwargs["parser_name"]
                    ReasoningParserManager.get_reasoning_parser(parser_name)

            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="qwen-0.5b",
                    model_source="Qwen/Qwen2.5-0.5B-Instruct",
                ),
                callback_config=dict(
                    callback_class=ReasoningParserCallback,
                    callback_kwargs=dict(parser_name="deepseek_r1"),
                ),
            )
    """

    pass


@PublicAPI(stability="alpha")
class CallbackConfig(_CallbackConfig):
    """Configuration for attaching a callback to an LLM deployment."""

    pass


@PublicAPI(stability="alpha")
class CloudDownloader(_CloudDownloader):
    """Built-in callback that downloads files from cloud storage (S3, GCS, Azure)
    before model initialization.

    Examples:
        .. testcode::
            :skipif: True

            from ray.serve.llm import LLMConfig, build_openai_app
            from ray.serve.llm.callbacks import CloudDownloader

            llm_config = LLMConfig(
                model_loading_config=dict(
                    model_id="my-model",
                    model_source="org/model-name",
                ),
                callback_config=dict(
                    callback_class=CloudDownloader,
                    callback_kwargs=dict(
                        paths=[
                            ("s3://my-bucket/tokenizer.json", "/tmp/model/tokenizer.json"),
                            ("s3://my-bucket/config.json", "/tmp/model/config.json"),
                        ]
                    ),
                ),
            )

            app = build_openai_app(dict(llm_configs=[llm_config]))
    """

    pass


__all__ = ["CallbackBase", "CallbackConfig", "CallbackCtx", "CloudDownloader"]
