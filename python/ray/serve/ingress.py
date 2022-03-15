from typing import Callable, Optional, Union

import starlette

from ray import serve
from ray.serve.api import DAGHandle


@serve.deployment
class DAGDriver:
    def __init__(
        self,
        dag_handle: DAGHandle,
        *,
        input_schema: Optional[Union[str, Callable]] = None,
    ):
        raise NotImplementedError()

    async def __call__(self, request: starlette.requests.Request):
        """Parse input schema and pass the result to the DAG handle."""
        raise NotImplementedError()
