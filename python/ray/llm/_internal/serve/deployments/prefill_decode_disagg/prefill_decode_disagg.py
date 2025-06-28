"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""
import logging
import uuid
from typing import Any, AsyncGenerator, Dict, Union

from pydantic import BaseModel, Field
from vllm.config import KVTransferConfig

from ray import serve
from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.configs.server_models import (
    LLMRawResponse,
    parse_args as parse_llm_configs,
)
from ray.llm._internal.serve.deployments.llm.llm_server import ResponsePostprocessor
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    KV_TRANSFER_PARAMS_KEY,
)
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import (
    LLMConfig,
    LLMRouter,
    LLMServer,
    ModelLoadingConfig,
    build_llm_deployment,
)

logger = logging.getLogger(__name__)


class PDServingArgs(BaseModel):
    """Schema for P/D serving args."""

    prefill_config: Union[str, LLMConfig]
    decode_config: Union[str, LLMConfig]
    proxy_deployment_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="""
            The Ray @server.deployment options for the proxy server.
        """,
    )

    def parse_args(self) -> "PDServingArgs":
        """Converts this LLMServingArgs object into an DeployArgs object."""

        def parse_configs_and_cast_type(config: Union[str, LLMConfig]) -> LLMConfig:
            # ray.serve.llm.__init__ imports internal LLMConfig, and extends it to external-facing LLMConfig.
            # parse_llm_configs returns internal LLMConfig, while {prefill, decode}_configs expect external-facing LLMConfig.
            # So the model_dump() here is to convert the type, to satisfy pydantic.
            # TODO(lk-chen): refactor llm_config parsing to avoid this model_dump, and make llm_config more reusable.
            config = parse_llm_configs([config])[0]
            return LLMConfig(**config.model_dump())

        return PDServingArgs(
            # Parse string file path into LLMConfig
            prefill_config=parse_configs_and_cast_type(self.prefill_config),
            decode_config=parse_configs_and_cast_type(self.decode_config),
            proxy_deployment_config=self.proxy_deployment_config,
        )


class PDProxyServer(LLMServer):
    _default_engine_cls = None
    """
    Proxy between P/D LLM servers.

    For chat and completions, proxy sends the request to the prefill server and
    then parses the response to send to the decode server.

    Args:
        llm_config: The LLM config for the proxy server, LLMRouter will use this config to
            setup the supported model list (/v1/models endpoint) and route request to proper
            server according to the model id.
        prefill_server: The prefill server deployment handle.
        decode_server: The decode server deployment handle.
    """

    async def __init__(
        self,
        llm_config: LLMConfig,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):

        # We pass `llm_config` here to let super() extract the model_id, such that /v1/models
        # endpoint can work correctly.
        # TODO(lk-chen): refactor LLMRouter <-> LLMServer such that router query model_id through
        # API, instead of passing it in as an argument.
        await super().__init__(
            llm_config,
        )

        self.prefill_server = prefill_server
        self.decode_server = decode_server

    async def _predict(
        self,
        request_id: str,
        prompt: Prompt,
        stream: bool,
    ) -> AsyncGenerator[LLMRawResponse, None]:
        """
        Disaggregate the P/D requests:
        1. Send the request to the prefill server.
        2. Parse the response and forward necessary fields to the decode server.
        3. Return the response from the decode server.
        """

        assert (
            prompt.parameters.get(KV_TRANSFER_PARAMS_KEY, None) is None
        ), f"{KV_TRANSFER_PARAMS_KEY} should be empty before proxy"
        prefill_prompt = prompt.model_copy(deep=True)
        prefill_prompt.parameters[KV_TRANSFER_PARAMS_KEY] = {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "remote_host": None,
            "remote_port": None,
        }
        prefill_prompt.parameters["max_tokens"] = 1

        prefill_response_gen: AsyncGenerator[
            LLMRawResponse, None
        ] = self.prefill_server.options(
            # _predict returns generator, we have to set stream=True
            stream=True
        )._predict.remote(
            request_id=request_id, prompt=prefill_prompt, stream=False
        )

        prefill_response = await ResponsePostprocessor.merge_stream(
            prefill_response_gen
        )

        if prefill_response.error:
            logger.error(f"Prefill server returned error: {prefill_response.error}")
            yield prefill_response
            return

        kv_transfer_params = prefill_response.metadata[KV_TRANSFER_PARAMS_KEY]
        logger.debug(
            f"Prefill metadata[{KV_TRANSFER_PARAMS_KEY}]: {kv_transfer_params}"
        )
        prompt.parameters[KV_TRANSFER_PARAMS_KEY] = kv_transfer_params

        async for chunk in self.decode_server.options(stream=True)._predict.remote(
            request_id=request_id, prompt=prompt, stream=stream
        ):
            yield chunk

    @classmethod
    def as_deployment(cls) -> serve.Deployment:
        """Turns PDProxyServer into a Ray Serve deployment."""
        return serve.deployment()(cls)


def build_app(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing P/D disaggregation."""

    pd_config = PDServingArgs.model_validate(pd_serving_args).parse_args()

    model_id = pd_config.decode_config.model_id
    assert model_id == pd_config.prefill_config.model_id, "P/D model id mismatch"

    for config in [pd_config.prefill_config, pd_config.decode_config]:
        if "kv_transfer_config" not in config.engine_kwargs:
            config.engine_kwargs.update(
                {
                    "kv_transfer_config": KVTransferConfig(
                        kv_connector="NixlConnector",
                        kv_role="kv_both",
                        engine_id=str(uuid.uuid4()),
                    )
                }
            )

    prefill_deployment = build_llm_deployment(
        pd_config.prefill_config, name_prefix="Prefill:"
    )
    decode_deployment = build_llm_deployment(
        pd_config.decode_config, name_prefix="Decode:"
    )

    proxy_server_deployment = (
        PDProxyServer.as_deployment()
        .options(**pd_config.proxy_deployment_config)
        .bind(
            llm_config=LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id=model_id)
            ),
            prefill_server=prefill_deployment,
            decode_server=decode_deployment,
        )
    )

    return LLMRouter.as_deployment().bind(llm_deployments=[proxy_server_deployment])
