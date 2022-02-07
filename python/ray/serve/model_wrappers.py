from abc import abstractclassmethod, abstractmethod
from typing import Any, Callable, Type, Union
from fastapi import Depends, FastAPI
import numpy as np
import pandas as pd
from ray.serve.http_util import ASGIHTTPSender
import starlette.requests
import torch
from ray import serve
from ray._private.utils import import_attr


DataBatchType = Union[np.ndarray, pd.DataFrame]


class BaseCheckpoint:
    @abstractclassmethod
    def from_uri(cls, uri: str) -> "BaseCheckpoint":
        pass

    @abstractmethod
    def load(self) -> Any:
        pass


class BaseModel:
    @abstractclassmethod
    def load_from_checkpoint(cls, checkpoint: BaseCheckpoint) -> "BaseModel":
        pass

    @abstractmethod
    def predict(self, inp: DataBatchType) -> Any:
        pass


class TorchCheckpoint:
    def __init__(self, on_disk_path: str):
        self.on_disk_path = on_disk_path

    @classmethod
    def from_uri(cls, uri: str) -> "TorchCheckpoint":
        return cls(uri)

    def load(self):
        return torch.load(self.on_disk_path)


class TorchModel:
    def __init__(self, nn_module) -> None:
        self.model = nn_module.eval()

    @classmethod
    def load_from_checkpoint(cls, checkpoint: TorchCheckpoint) -> "TorchModel":
        return cls(checkpoint.load())

    def predict(self, inp: np.ndarray) -> np.ndarray:
        with torch.inference_mode():
            out = self.model(inp)
        return out.numpy().tolist()


# TODO(somehow infer the name and route_prefix for the Model)
class ModelWrapper:
    def __init__(
        self,
        model_cls: Type[BaseModel],
        checkpoint: BaseCheckpoint,
        input_schema: Union[None, str, Callable[[Any], DataBatchType]] = None,
    ) -> None:
        self.model = model_cls.load_from_checkpoint(checkpoint)
        if input_schema is None:
            from ray.serve.http_adapters import serve_api_resolver

            input_schema = serve_api_resolver
        elif isinstance(input_schema, str):
            input_schema = import_attr(input_schema)
        assert callable(input_schema), "input schema must be callable"

        self.app = FastAPI()
        # self.app.add_api_route("/", self.handle_request, methods=["POST"])

        @self.app.post("/")
        @serve.batch(max_batch_size=8, batch_wait_timeout_s=3)
        async def handle_request(inp=Depends(input_schema)):
            # TODO(simon): implement custom JSON encoder for FastAPI to handle common type
            print(f"Got batch size of {len(inp)}")
            return self.model.predict(inp)

        # Inject the input schema as FastAPI dependencies
        # sig = inspect.signature(self.handle_request.__func__)
        # inp_param = sig.parameters["inp"]
        # new_param = inp_param.replace(default=Depends(input_schema))
        # setattr(
        #     self.handle_request.__func__,
        #     "__signature__",
        #     sig.replace(parameters=[new_param]),
        # )

        # # Build FastAPI app

    async def __call__(self, request: starlette.requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


@serve.deployment
class TorchModelWrapper(ModelWrapper):
    def __init__(
        self,
        checkpoint_uri: str,
        input_schema: str = "ray.serve.http_adapters.serve_api_resolver",
    ) -> None:
        super().__init__(
            TorchModel,
            TorchCheckpoint.from_uri(checkpoint_uri),
            input_schema=input_schema,
        )
