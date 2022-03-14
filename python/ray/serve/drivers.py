from typing import Callable, Optional, Union

import starlette

from ray import serve
from ray.serve.api import DeployedCallGraph


@serve.deployment
class PipelineDriver:
    def __init__(
        self,
        call_graph: DeployedCallGraph,
        *,
        input_schema: Optional[Union[str, Callable]] = None,
    ):
        raise NotImplementedError()

    async def __call__(self, request: starlette.requests.Request):
        """Parse input schema and pass the result to the call graph."""
        raise NotImplementedError()
