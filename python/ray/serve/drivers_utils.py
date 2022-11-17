import inspect
from fastapi import Body
from typing import Any, Callable, Optional, Type, Union
from pydantic import BaseModel
from ray._private.utils import import_attr
from ray.util.annotations import DeveloperAPI

HTTPAdapterFn = Callable[[Any], Any]


@DeveloperAPI
def load_http_adapter(
    http_adapter: Optional[Union[str, HTTPAdapterFn, Type[BaseModel]]]
) -> HTTPAdapterFn:

    if http_adapter is None:
        http_adapter = "ray.serve.http_adapters.starlette_request"

    if isinstance(http_adapter, str):
        http_adapter = import_attr(http_adapter)

    if inspect.isclass(http_adapter) and issubclass(http_adapter, BaseModel):

        def http_adapter(inp: http_adapter = Body(...)):
            return inp

    if not inspect.isfunction(http_adapter):
        raise ValueError(
            "input schema must be a callable function or pydantic model class."
        )

    if any(
        param.annotation == inspect.Parameter.empty
        for param in inspect.signature(http_adapter).parameters.values()
    ):
        raise ValueError("input schema function's signature should be type annotated.")
    return http_adapter
