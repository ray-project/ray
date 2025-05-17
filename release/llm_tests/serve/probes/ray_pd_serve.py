"""Using Ray Serve to proxy between P/D LLM servers.

This shows how generic and extendible the Ray Serve LLM library is.
"""

import os
import asyncio
import sys
from typing import List, AsyncGenerator

if sys.version_info >= (3, 11):
    from asyncio import timeout
else:
    from async_timeout import timeout

from vllm.config import KVTransferConfig

from ray import serve
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle
from ray.serve.llm import LLMRouter

from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.llm._internal.serve.deployments.utils.server_utils import peek_at_generator
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_HEALTH_CHECK_PERIOD_S,
    DEFAULT_HEALTH_CHECK_TIMEOUT_S,
    RAYLLM_ROUTER_HTTP_TIMEOUT,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMServingArgs,
    LLMConfig,
    LLMRawResponse,
)

# TODO(lk-chen): public api does not have name_prefix, shall we add it to LLMConfig?
from ray.llm._internal.serve.builders.application_builders import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class PDProxyServer(LLMServer):
    """
    Proxy between P/D LLM servers.

    For chat and completions, proxy sends the request to the prefill server and
    then parses the response to send to the decode server.

    For embeddings, proxy sends the request to the prefill server and returns the
    response directly.
    """

    async def __init__(
        self,
        llm_config: LLMConfig,
        prefill_server: DeploymentHandle,
        decode_server: DeploymentHandle,
    ):
        class FakeEngine:
            def __init__(self, *args, **kwargs):
                pass

            async def start(self, *args, **kwargs):
                pass

        await super().__init__(llm_config, engine_cls=FakeEngine)

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
        logger.info(f"clkbp {prompt.parameters=}")

        from ray.llm._internal.serve.deployments.llm.vllm import KV_TRANSFER_PARAMS_KEY

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
        logger.info(f"clkbp {prompt.parameters=}")
        logger.info("clkbp before prefill")

        async def prefill_response_gen() -> AsyncGenerator[LLMRawResponse, None]:
            async with timeout(RAYLLM_ROUTER_HTTP_TIMEOUT):
                async for chunk in self.prefill_server.options(
                    stream=True
                )._predict.remote(
                    request_id=request_id, prompt=prefill_prompt, stream=False
                ):
                    yield chunk

        logger.info("clkbp after prefill")
        logger.info(f"clkbp {prompt.parameters=}")
        prefill_response, prefill_response_gen = await peek_at_generator(
            prefill_response_gen()
        )
        if prefill_response.error:
            logger.error(f"Prefill server returned error: {prefill_response.error}")
            async for chunk in prefill_response_gen:
                yield chunk
            return
        from ray.llm._internal.serve.deployments.llm.vllm import KV_TRANSFER_PARAMS_KEY

        prompt.parameters[
            KV_TRANSFER_PARAMS_KEY
        ] = prefill_response.internal_parameters[KV_TRANSFER_PARAMS_KEY]

        import time

        start_time = time.perf_counter()
        logger.info(f"clkbp {prompt.parameters=}")
        async for chunk in self.decode_server.options(stream=True)._predict.remote(
            request_id=request_id, prompt=prompt, stream=stream
        ):
            yield chunk
        logger.info("clkbp after decode")
        prefill_end = time.perf_counter()
        # TODO(lk-chen): propagate prefill time.

    async def check_health(self) -> bool:
        """Check the health of the llm engine."""
        are_healthy = await asyncio.gather(
            self.prefill_server.check_health.remote(),
            self.decode_server.check_health.remote(),
        )
        return all(are_healthy)

    @classmethod
    def as_deployment(cls) -> serve.Deployment:
        """Turns PDProxyServer into a Ray Serve deployment.

        Usage:
        >>> prefill_deploy = ray.serve.llm.build_llm_deployment(prefill_config)
        >>> decode_deploy = ray.serve.llm.build_llm_deployment(decode_config)
        >>> PDProxyServer.as_deployment().bind(
        ...     llm_config=llm_config,
        ...     prefill_server=prefill_deploy,
        ...     decode_server=decode_deploy,
        ... )
        >>> ray.serve.run(deployment)
        """
        return serve.deployment(
            autoscaling_config={
                "min_replicas": 1,
                "initial_replicas": 1,
                "max_replicas": 10,
                "target_ongoing_requests": int(
                    os.environ.get(
                        "RAYLLM_ROUTER_TARGET_ONGOING_REQUESTS",
                        os.environ.get(
                            "RAYLLM_ROUTER_TARGET_NUM_ONGOING_REQUESTS_PER_REPLICA", 10
                        ),
                    )
                ),
            },
            max_ongoing_requests=20,  # Maximum backlog for a single replica
            health_check_period_s=DEFAULT_HEALTH_CHECK_PERIOD_S,
            health_check_timeout_s=DEFAULT_HEALTH_CHECK_TIMEOUT_S,
        )(cls)


def build_PD_disagg_app(llm_serving_args: LLMServingArgs) -> Application:
    rayllm_args = LLMServingArgs.model_validate(llm_serving_args).parse_args()

    prefill_configs = rayllm_args.llm_configs
    model_ids = {m.model_id for m in prefill_configs}
    if len(model_ids) != len(prefill_configs):
        raise ValueError("Duplicate models found. Make sure model ids are unique.")

    if len(prefill_configs) == 0:
        logger.error(
            "List of models is empty. Maybe some parameters cannot be parsed into the LLMConfig config."
        )

    for config in prefill_configs:
        config.engine_kwargs.update(
            {
                "kv_transfer_config": KVTransferConfig(
                    kv_connector="NixlConnector", kv_role="kv_both"
                )
            }
        )

    # build_llm_deployment() could change config, so deep copy configs for P/D.
    # llm_configs should be list[`LLMConfig`] at this point.
    decode_configs = [c.model_copy(deep=True) for c in prefill_configs]

    def _get_llm_deployments(
        llm_configs: List[LLMConfig], name_prefix: str
    ) -> List[DeploymentHandle]:
        return [
            build_llm_deployment(llm_config, name_prefix=name_prefix)
            for llm_config in llm_configs
        ]

    prefill_deployments = _get_llm_deployments(prefill_configs, "PrefillDeployment")
    decode_deployments = _get_llm_deployments(decode_configs, "DecodeDeployment")
    if len(prefill_deployments) != len(decode_deployments):
        raise ValueError("Prefill and decode deployments must have the same length.")

    proxy_server_deployments = [
        PDProxyServer.as_deployment().bind(
            llm_config=llm_config,
            prefill_server=prefill_deploy,
            decode_server=decode_deploy,
        )
        for llm_config, prefill_deploy, decode_deploy in zip(
            prefill_configs, prefill_deployments, decode_deployments
        )
    ]

    # TODO(lk-chen): Need to re-think about configs here, in non-PD case LLMRouter use 2x replica
    # as LLM, here in PD case what shall we do?
    return LLMRouter.as_deployment(llm_configs=decode_configs).bind(
        llm_deployments=proxy_server_deployments
    )
