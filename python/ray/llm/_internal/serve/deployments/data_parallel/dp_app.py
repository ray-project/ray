from typing import Union

from pydantic import BaseModel

from ray.llm._internal.serve.deployments.data_parallel.dp_llm_server import DPLLMServer
from ray.llm._internal.serve.deployments.data_parallel.dp_rank_assigner import (
    DPRankAssigner,
)
from ray.serve.deployment import Application
from ray.serve.llm import LLMConfig, LLMRouter


class DPServingArgs(BaseModel):
    """Schema for DP serving args."""

    llm_config: Union[str, LLMConfig]
    dp_size: int


def build_dp_openai_app(dp_serving_args: dict) -> Application:
    dp_config = DPServingArgs.model_validate(dp_serving_args).parse_args()

    dp_rank_assigner = DPRankAssigner.bind(dp_size=dp_config.dp_size)

    dp_llm_server = DPLLMServer.bind(
        llm_config=dp_config.llm_config, dp_rank_assigner=dp_rank_assigner
    )

    router = LLMRouter.as_deployment(llm_config=[dp_config.llm_config]).bind(
        [dp_llm_server]
    )

    return router
