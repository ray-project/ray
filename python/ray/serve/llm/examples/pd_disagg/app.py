"""Using Ray Serve to deploy LLM models with P/D disaggregation.
"""

import os
import asyncio
import sys
import logging
from typing import List, AsyncGenerator, Union, cast

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

from pydantic import BaseModel
from vllm.config import KVTransferConfig

from ray import serve
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import (
    LLMRouter,
    LLMConfig,
    ModelLoadingConfig,
    build_llm_deployment,
    LLMServer,
)

from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    KV_TRANSFER_PARAMS_KEY,
)
from ray.llm._internal.serve.deployments.utils.server_utils import peek_at_generator
from ray.llm._internal.serve.configs.constants import (
    RAYLLM_ROUTER_HTTP_TIMEOUT,
)
from ray.llm._internal.serve.configs.server_models import (
    parse_args as parse_llm_configs,
    LLMRawResponse,
)

logger = logging.getLogger(__name__)


class PDServingArgs(BaseModel):
    """Schema for P/D serving args."""

    prefill_configs: List[Union[str, LLMConfig]]
    decode_configs: List[Union[str, LLMConfig]]

    def parse_args(self) -> "PDServingArgs":
        """Converts this LLMServingArgs object into an DeployArgs object."""

        if len(self.prefill_configs) != len(self.decode_configs):
            raise ValueError(
                f"Prefill and decode configs must have the same length, got {len(self.prefill_configs)} prefill configs and {len(self.decode_configs)} decode configs."
            )

        def parse_configs_and_cast_type(
            configs: List[Union[str, LLMConfig]]
        ) -> List[LLMConfig]:
            # ray.serve.llm.__init__ imports internal LLMConfig, and extends it to external-facing LLMConfig.
            # parse_llm_configs returns internal LLMConfig, while {prefill, decode}_configs expect external-facing LLMConfig.
            # So the model_dump() here is to convert the type, to satisfy pydantic.
            configs = parse_llm_configs(configs)
            return [LLMConfig(**config.model_dump()) for config in configs]

        return PDServingArgs(
            # Parse string file path into LLMConfig
            prefill_configs=parse_configs_and_cast_type(self.prefill_configs),
            decode_configs=parse_configs_and_cast_type(self.decode_configs),
        )


class PDProxyServer(LLMServer):
    """
    Proxy between P/D LLM servers.

    For chat and completions, proxy sends the request to the prefill server and
    then parses the response to send to the decode server.
    """

    async def __init__(
        self,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):
        class FakeEngine:
            """Provide a fake engine such that proxy don't really start any engine."""

            def __init__(self, *args, **kwargs):
                pass

            async def start(self, *args, **kwargs):
                pass

        await super().__init__(
            LLMConfig(model_loading_config=ModelLoadingConfig(model_id="")),
            engine_cls=FakeEngine,
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

        async def prefill_response_gen() -> AsyncGenerator[LLMRawResponse, None]:
            async with timeout(RAYLLM_ROUTER_HTTP_TIMEOUT):
                async for chunk in self.prefill_server.options(
                    stream=True
                )._predict.remote(
                    request_id=request_id, prompt=prefill_prompt, stream=False
                ):
                    yield chunk

        prefill_response, prefill_response_gen = await peek_at_generator(
            prefill_response_gen()
        )
        if prefill_response.error:
            logger.error(f"Prefill server returned error: {prefill_response.error}")
            async for chunk in prefill_response_gen:
                yield chunk
            return

        prompt.parameters[
            KV_TRANSFER_PARAMS_KEY
        ] = prefill_response.internal_parameters[KV_TRANSFER_PARAMS_KEY]

        import time

        start_time = time.perf_counter()
        async for chunk in self.decode_server.options(stream=True)._predict.remote(
            request_id=request_id, prompt=prompt, stream=stream
        ):
            yield chunk
        prefill_end = time.perf_counter()
        # TODO(lk-chen): propagate prefill time.

    async def check_health(self) -> None:
        """Check the health of the llm engine."""
        are_healthy = await asyncio.gather(
            self.prefill_server.check_health.remote(),
            self.decode_server.check_health.remote(),
        )
        if not all(are_healthy):
            raise RuntimeError("LLM server is unhealthy.")

    @classmethod
    def as_deployment(cls) -> serve.Deployment:
        """Turns PDProxyServer into a Ray Serve deployment.

        Usage:
        >>> prefill_deploy = ray.serve.llm.build_llm_deployment(prefill_config)
        >>> decode_deploy = ray.serve.llm.build_llm_deployment(decode_config)
        >>> PDProxyServer.as_deployment().bind(
        ...     prefill_server=prefill_deploy,
        ...     decode_server=decode_deploy,
        ... )
        >>> ray.serve.run(deployment)
        """
        return serve.deployment()(cls)


def _validate_llm_configs(llm_configs: List[LLMConfig], config_name: str) -> None:
    model_ids = {m.model_id for m in llm_configs}
    if len(model_ids) != len(llm_configs):
        raise ValueError(
            f"Duplicate models found. Make sure model ids are unique for {config_name}."
        )
    if len(llm_configs) == 0:
        logger.error(
            f"List of models is empty. Maybe some parameters cannot be parsed into the LLMConfig config for {config_name}."
        )


def build_app(pd_serving_args: dict) -> Application:
    """Build a deployable application utilizing P/D disaggregation."""

    rayllm_args = PDServingArgs.model_validate(pd_serving_args).parse_args()

    _validate_llm_configs(rayllm_args.prefill_configs, "prefill_configs")
    _validate_llm_configs(rayllm_args.decode_configs, "decode_configs")

    for config in rayllm_args.prefill_configs + rayllm_args.decode_configs:
        if "kv_transfer_config" not in config.engine_kwargs:
            config.engine_kwargs.update(
                {
                    "kv_transfer_config": KVTransferConfig(
                        kv_connector="NixlConnector", kv_role="kv_both"
                    )
                }
            )

    def _get_llm_deployments(llm_configs: List[LLMConfig]) -> List[DeploymentHandle]:
        return [build_llm_deployment(llm_config) for llm_config in llm_configs]

    prefill_deployments = _get_llm_deployments(rayllm_args.prefill_configs)
    decode_deployments = _get_llm_deployments(rayllm_args.decode_configs)
    if len(prefill_deployments) != len(decode_deployments):
        raise ValueError("Prefill and decode deployments must have the same length.")

    proxy_server_deployments = [
        PDProxyServer.as_deployment().bind(
            prefill_server=prefill_deploy,
            decode_server=decode_deploy,
        )
        for prefill_deploy, decode_deploy in zip(
            prefill_deployments, decode_deployments
        )
    ]

    # TODO(lk-chen): Need to re-think about configs here, in non-PD case LLMRouter use 2x replica
    # as LLM, here in PD case what shall we do?
    return LLMRouter.as_deployment(llm_configs=rayllm_args.decode_configs).bind(
        llm_deployments=proxy_server_deployments
    )
