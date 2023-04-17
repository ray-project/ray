from collections import defaultdict
import inspect
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union
from pydantic import BaseModel
import numpy as np
from abc import abstractmethod
import starlette
from fastapi import Depends, FastAPI

import ray
from ray import serve
from ray._private.utils import import_attr
from ray.serve._private.utils import require_packages
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.serve.drivers_utils import load_http_adapter, HTTPAdapterFn
from ray.serve._private.utils import install_serve_encoders_to_fastapi
from ray.serve._private.http_util import ASGIHTTPSender


if TYPE_CHECKING:
    from ray.train.predictor import Predictor
    from ray.air.checkpoint import Checkpoint


try:
    import pandas as pd
except ImportError:
    pd = None

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _load_checkpoint(
    checkpoint: Union["Checkpoint", str],
) -> "Checkpoint":
    from ray.air.checkpoint import Checkpoint

    if isinstance(checkpoint, str):
        checkpoint = Checkpoint.from_uri(checkpoint)
    assert isinstance(checkpoint, Checkpoint)
    return checkpoint


def _load_predictor_cls(
    predictor_cls: Union[str, Type["Predictor"]],
) -> Type["Predictor"]:
    from ray.train.predictor import Predictor

    if isinstance(predictor_cls, str):
        predictor_cls = import_attr(predictor_cls)
    if Predictor is not None and not issubclass(predictor_cls, Predictor):
        raise ValueError(
            f"{predictor_cls} class must be a subclass of ray.air `Predictor`"
        )
    return predictor_cls


def _unpack_dataframe_to_serializable(output_df: "pd.DataFrame") -> "pd.DataFrame":
    """Unpack predictor's pandas return value to JSON serializable format.

    In dl_predictor.py we return a pd.DataFrame that could have multiple
    columns but value of each column is a TensorArray. Flatten the
    TensorArray to list to ensure output is json serializable as http
    response.

    In numpy predictor path, we might return collection of np.ndarrays that also
    requires flattening to list to ensure output is json serializable as http
    response.
    """
    from ray.data.extensions import TensorDtype

    for col in output_df.columns:
        # TensorArray requires special handling to numpy array.
        if isinstance(output_df.dtypes[col], TensorDtype):
            output_df[col] = list(output_df[col].to_numpy())
        # # DL predictor outputs raw ndarray outputs as opaque numpy object.
        # # ex: output_df = pd.DataFrame({"predictions": [np.array(1)]})
        elif output_df.dtypes[col] == np.dtype(object) and all(
            isinstance(v, np.ndarray) for v in output_df[col]
        ):
            output_df.loc[:, col] = [v.tolist() for v in output_df[col]]
    return output_df


class _BatchingManager:
    """A collection of utilities for batching and splitting data."""

    @staticmethod
    def batch_array(input_list: List[np.ndarray]) -> np.ndarray:
        batched = np.stack(input_list)
        return batched

    @staticmethod
    def split_array(output_array: np.ndarray, batch_size: int) -> List[np.ndarray]:
        if not isinstance(output_array, np.ndarray):
            raise TypeError(
                f"The output should be np.ndarray but Serve got {type(output_array)}."
            )
        if len(output_array) != batch_size:
            raise ValueError(
                f"The output array should have shape of ({batch_size}, ...) "
                f"but Serve got {output_array.shape}"
            )
        return [
            arr.squeeze(axis=0) for arr in np.split(output_array, batch_size, axis=0)
        ]

    @staticmethod
    @require_packages(["pandas"])
    def batch_dataframe(input_list: List["pd.DataFrame"]) -> "pd.DataFrame":
        import pandas as pd

        batched = pd.concat(input_list, axis="index", ignore_index=True, copy=False)
        return batched

    @staticmethod
    @require_packages(["pandas"])
    def split_dataframe(
        output_df: "pd.DataFrame", batch_size: int
    ) -> List["pd.DataFrame"]:
        if not isinstance(output_df, pd.DataFrame):
            raise TypeError(
                "The output should be a Pandas DataFrame but Serve got "
                f"{type(output_df)}"
            )
        if len(output_df) % batch_size != 0:
            raise ValueError(
                f"The output dataframe should have length divisible by {batch_size}, "
                f"but Serve got length {len(output_df)}."
            )

        output_df = _unpack_dataframe_to_serializable(output_df)

        return [df.reset_index(drop=True) for df in np.split(output_df, batch_size)]

    @staticmethod
    def batch_dict_array(
        input_list: List[Dict[str, np.ndarray]]
    ) -> Dict[str, np.ndarray]:
        batch_size = len(input_list)

        # Check that all inputs have the same dict keys.
        input_keys = [set(item.keys()) for item in input_list]
        batch_has_same_keys = input_keys.count(input_keys[0]) == batch_size
        if not batch_has_same_keys:
            raise ValueError(
                "The input batch's dictoinary must contain the same keys. "
                f"Got different keys in some dictionaries: {input_keys}."
            )

        # Turn list[dict[str, array]] to dict[str, List[array]]
        key_to_list = defaultdict(list)
        for single_dict in input_list:
            for key, arr in single_dict.items():
                key_to_list[key].append(arr)

        # Turn dict[str, List[array]] to dict[str, array]
        batched_dict = {}
        for key, list_of_arr in key_to_list.items():
            arr = _BatchingManager.batch_array(list_of_arr)
            batched_dict[key] = arr

        return batched_dict

    @staticmethod
    def split_dict_array(
        output_dict: Dict[str, np.ndarray], batch_size: int
    ) -> List[Dict[str, np.ndarray]]:
        if isinstance(output_dict, list):
            return output_dict

        if not isinstance(output_dict, Dict):
            raise TypeError(
                f"The output should be a dictionary but Serve got {type(output_dict)}."
            )

        split_list_of_dict = [{} for _ in range(batch_size)]
        for key, result_arr in output_dict.items():
            split_arrays = _BatchingManager.split_array(result_arr, batch_size)
            # in place update each dictionary with the split array chunk.
            for item, arr in zip(split_list_of_dict, split_arrays):
                item[key] = arr

        return split_list_of_dict


@DeveloperAPI
class SimpleSchemaIngress:
    def __init__(
        self, http_adapter: Optional[Union[str, HTTPAdapterFn, Type[BaseModel]]] = None
    ):
        """Create a FastAPI endpoint annotated with http_adapter dependency.

        Args:
            http_adapter(str, HTTPAdapterFn, None, Type[pydantic.BaseModel]):
              The FastAPI input conversion function or a pydantic model class.
              By default, Serve will directly pass in the request object
              starlette.requests.Request. You can pass in any FastAPI dependency
              resolver. When you pass in a string, Serve will import it.
              Please refer to Serve HTTP adatper documentation to learn more.
        """
        install_serve_encoders_to_fastapi()
        http_adapter = load_http_adapter(http_adapter)
        self.app = FastAPI()

        @self.app.get("/")
        @self.app.post("/")
        async def handle_request(inp=Depends(http_adapter)):
            resp = await self.predict(inp)
            return resp

    @abstractmethod
    async def predict(self, inp):
        raise NotImplementedError()

    async def __call__(self, request: starlette.requests.Request):
        # NOTE(simon): This is now duplicated from ASGIAppWrapper because we need to
        # generate FastAPI on the fly, we should find a way to unify the two.
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


@PublicAPI(stability="alpha")
class PredictorWrapper(SimpleSchemaIngress):
    """Serve any Ray AIR predictor from an AIR checkpoint.

    Args:
        predictor_cls: The class or path for predictor class.
            The type must be a subclass of :class:`ray.train.predictor.Predictor`.
        checkpoint: The checkpoint object or a uri to load checkpoint
            from

            - The checkpoint object must be an instance of
              :class:`ray.air.checkpoint.Checkpoint`.
            - The uri string will be called to construct a checkpoint object using
              ``Checkpoint.from_uri("uri_to_load_from")``.

        http_adapter: The FastAPI input conversion
            function. By default, Serve will use the
            :ref:`NdArray <serve-ndarray-schema>` schema and convert to numpy array.
            You can pass in any FastAPI dependency resolver that returns
            an array. When you pass in a string, Serve will import it.
            Please refer to :ref:`Serve HTTP adatpers <serve-http-adapters>`
            documentation to learn more.
        batching_params: override the default parameters to
            :func:`ray.serve.batch`. Pass ``False`` to disable batching.
        predict_kwargs: optional keyword arguments passed to the
            ``Predictor.predict`` method upon each call.
        **predictor_from_checkpoint_kwargs: Additional keyword arguments passed to the
            ``Predictor.from_checkpoint()`` call.
    """

    def __init__(
        self,
        predictor_cls: Union[str, Type["Predictor"]],
        checkpoint: Union["Checkpoint", str],
        http_adapter: Union[
            str, HTTPAdapterFn
        ] = "ray.serve.http_adapters.json_to_ndarray",
        batching_params: Optional[Union[Dict[str, int], bool]] = None,
        predict_kwargs: Optional[Dict[str, Any]] = None,
        **predictor_from_checkpoint_kwargs,
    ):
        predictor_cls = _load_predictor_cls(predictor_cls)
        checkpoint = _load_checkpoint(checkpoint)

        # Automatic set use_gpu in predictor constructor if user provided
        # explicit GPU resources
        if (
            "use_gpu" in inspect.signature(predictor_cls.from_checkpoint).parameters
            and "use_gpu" not in predictor_from_checkpoint_kwargs
            and len(ray.get_gpu_ids()) > 0
        ):
            logger.info(
                "GPU resources identified in `PredictorDeployment`. "
                "Automatically enabling GPU prediction for this predictor. To "
                "disable set `use_gpu` to `False` in `PredictorDeployment`."
            )
            predictor_from_checkpoint_kwargs["use_gpu"] = True

        self.model = predictor_cls.from_checkpoint(
            checkpoint, **predictor_from_checkpoint_kwargs
        )

        predict_kwargs = predict_kwargs or dict()

        # Configure Batching
        if batching_params is False:

            async def predict_impl(inp: Union[np.ndarray, "pd.DataFrame"]):
                out = self.model.predict(inp, **predict_kwargs)
                if isinstance(out, ray.ObjectRef):
                    out = await out
                elif pd is not None and isinstance(out, pd.DataFrame):
                    out = _unpack_dataframe_to_serializable(out)
                return out

        else:
            batching_params = batching_params or dict()

            @serve.batch(**batching_params)
            async def predict_impl(inp: Union[List[np.ndarray], List["pd.DataFrame"]]):
                batch_size = len(inp)
                if isinstance(inp[0], np.ndarray):
                    batched = _BatchingManager.batch_array(inp)
                elif pd is not None and isinstance(inp[0], pd.DataFrame):
                    batched = _BatchingManager.batch_dataframe(inp)
                elif isinstance(inp[0], dict):
                    batched = _BatchingManager.batch_dict_array(inp)
                else:
                    raise ValueError(
                        "Predictor only accepts numpy array, dataframe, or dict of "
                        "arrays as input "
                        f"but got types {[type(i) for i in inp]}"
                    )

                out = self.model.predict(batched, **predict_kwargs)
                if isinstance(out, ray.ObjectRef):
                    out = await out

                if isinstance(out, np.ndarray):
                    return _BatchingManager.split_array(out, batch_size)
                elif pd is not None and isinstance(out, pd.DataFrame):
                    return _BatchingManager.split_dataframe(out, batch_size)
                elif isinstance(out, dict):
                    return _BatchingManager.split_dict_array(out, batch_size)
                elif isinstance(out, list) and len(out) == batch_size:
                    return out
                else:
                    raise ValueError(
                        f"Predictor only accepts list of length {batch_size}, numpy "
                        "array, dataframe, or dict of array as output "
                        f"but got types {type(out)} with length "
                        f"{len(out) if hasattr(out, '__len__') else 'unknown'}."
                    )

        self.predict_impl = predict_impl

        super().__init__(http_adapter)

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.predict_impl(inp)

    def reconfigure(self, config):
        """Reconfigure model from config checkpoint"""
        from ray.air.checkpoint import Checkpoint

        predictor_cls = _load_predictor_cls(config["predictor_cls"])
        self.model = predictor_cls.from_checkpoint(
            Checkpoint.from_dict(config["checkpoint"])
        )


@serve.deployment
class PredictorDeployment(PredictorWrapper):
    """Ray Serve Deployment for AIRPredictorWrapper."""
