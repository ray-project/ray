import inspect
from typing import Any, Callable, Dict, Optional, Type, Union

import starlette.requests
from fastapi import Depends, FastAPI

from ray._private.utils import import_attr
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.serve.http_util import ASGIHTTPSender
from ray import serve
import ray

DEFAULT_INPUT_SCHEMA = "ray.serve.http_adapters.array_to_databatch"
InputSchemaFn = Callable[[Any], DataBatchType]


def _load_input_schema(
    input_schema: Optional[Union[str, InputSchemaFn]]
) -> InputSchemaFn:
    if input_schema is None:
        input_schema = DEFAULT_INPUT_SCHEMA

    if isinstance(input_schema, str):
        input_schema = import_attr(input_schema)

    assert inspect.isfunction(input_schema), "input schema must be a callable function."
    return input_schema


def _load_checkpoint(
    checkpoint: Union[Checkpoint, Dict],
) -> Checkpoint:
    if isinstance(checkpoint, dict):
        user_keys = set(checkpoint.keys())
        expected_keys = {"checkpoint_cls", "uri"}
        if user_keys != expected_keys:
            raise ValueError(
                "The `checkpoint` dictionary is expects keys "
                f"{expected_keys} but got {user_keys}"
            )
        checkpoint = import_attr(checkpoint["checkpoint_cls"]).from_uri(
            checkpoint["uri"]
        )
    assert isinstance(checkpoint, Checkpoint)
    return checkpoint


def _load_predictor_cls(
    predictor_cls: Union[str, Type[Predictor]],
) -> Type[Predictor]:
    if isinstance(predictor_cls, str):
        predictor_cls = import_attr(predictor_cls)
    if not issubclass(predictor_cls, Predictor):
        raise ValueError(
            f"{predictor_cls} class must be a subclass of ray.ml `Predictor`"
        )
    return predictor_cls


class ModelWrapper:
    def __init__(
        self,
        predictor_cls: Union[str, Type[Predictor]],
        checkpoint: Union[Checkpoint, Dict],
        input_schema: Optional[Union[str, InputSchemaFn]] = None,
        batching_params: Optional[Union[Dict[str, int], bool]] = None,
    ):
        """Serve any Ray ML predictor from checkpoint.

        Args:
            predictor_cls(str, Type[Predictor]): The class or path for predictor class.
              The type must be a subclass of ray.ml `Predictor`.
            checkpoint(Checkpoint, dict): The checkpoint object or a dictionary describe
              the object.
                - The checkpoint object must be a subclass of ray.ml `Checkpoint`.
                - The dictionary should be in the form of
                  {"checkpoint_cls": "import.path.MyCheckpoint",
                   "uri": "uri_to_load_from"}.
                  Serve will then call `MyCheckpoint.from_uri("uri_to_load_from")` to
                  instantiate the object.
            input_schema(str, InputSchemaFn, None): The FastAPI input conversion
              function. By default, Serve will use the `NdArray` schema and convert to
              numpy array. You can pass in any FastAPI dependency resolver that returns
              an array. When you pass in a string, Serve will import it.
              Please refer to Serve HTTP adatper documentation to learn more.
            batching_params(dict, None, False): override the default parameters to
              serve.batch. Pass `False` to disable batching.
        """
        predictor_cls = _load_predictor_cls(predictor_cls)
        checkpoint = _load_checkpoint(checkpoint)

        self.model = predictor_cls.from_checkpoint(checkpoint)
        self.app = FastAPI()

        # Configure Batching
        if batching_params is False:
            # Inject noop decorator to disable batching
            batching_decorator = lambda f: f  # noqa: E731
        else:
            batching_params = batching_params or dict()
            batching_decorator = serve.batch(**batching_params)

        @batching_decorator
        async def batched_predict(inp):
            out = self.model.predict(inp)
            if isinstance(out, ray.ObjectRef):
                out = await out
            return out

        self.batched_predict = batched_predict

        # Configure Input Schema
        input_schema = _load_input_schema(input_schema)

        @self.app.get("/")
        @self.app.post("/")
        async def handle_request(inp=Depends(input_schema)):
            return await batched_predict(inp)

    async def __call__(self, request: starlette.requests.Request):
        # NOTE(simon): This is now duplicated from ASGIAppWrapper because we need to
        # generate FastAPI on the fly, we should find a way to unify the two.
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()

    async def predict(self, inp):
        """Performing inference directly without HTTP."""
        return await self.batched_predict(inp)
