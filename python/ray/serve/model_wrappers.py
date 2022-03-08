from typing import Any, Callable, Dict, Optional, Type, Union

import starlette.requests
from fastapi import Depends, FastAPI

from ray._private.utils import import_attr
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.serve.http_util import ASGIHTTPSender
from ray import serve

DEFAULT_INPUT_SCHEMA = "ray.serve.http_adapters.array_to_databatch"
InputSchemaFn = Callable[[Any], DataBatchType]


def _load_input_schema(
    input_schema: Optional[Union[str, InputSchemaFn]]
) -> InputSchemaFn:
    if input_schema is None:
        input_schema = DEFAULT_INPUT_SCHEMA

    if isinstance(input_schema, str):
        input_schema = import_attr(input_schema)

    assert callable(input_schema), "input schema must be callable"
    return input_schema


class ModelWrapper:
    def __init__(
        self,
        predictor_cls: Type[Predictor],
        checkpoint: Checkpoint,
        input_schema: Optional[Union[str, InputSchemaFn]] = None,
        batching_params: Optional[Dict[str, int]] = None,
    ):
        self.model = predictor_cls.load_from_checkpoint(checkpoint)
        self.app = FastAPI()

        input_schema = _load_input_schema(input_schema)
        batching_params = batching_params or dict(
            max_batch_size=8, batch_wait_timeout_s=0.01
        )

        @self.app.post("/predict")
        @serve.batch(**batching_params)
        async def handle_request(inp=Depends(input_schema)):
            return self.model.predict(inp)

    async def __call__(self, request: starlette.requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()
