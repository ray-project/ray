from typing import Callable, Dict, List, Optional, Tuple, Type, Union
import numpy as np

from ray._private.utils import import_attr
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor
from ray.serve.drivers import HTTPAdapterFn, SimpleSchemaIngress
import ray
from ray import serve
from ray.serve.utils import require_packages

try:
    import pandas as pd
except ImportError:
    pd = None


def _load_checkpoint(
    checkpoint: Union[Checkpoint, str],
) -> Checkpoint:
    if isinstance(checkpoint, str):
        checkpoint = Checkpoint.from_uri(checkpoint)
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


def collate_array(
    input_list: List[np.ndarray],
) -> Tuple[np.ndarray, Callable[[np.ndarray], List[np.ndarray]]]:
    batch_size = len(input_list)
    batched = np.stack(input_list)

    def unpack(output_arr):
        if isinstance(output_arr, list):
            return output_arr
        assert isinstance(
            output_arr, np.ndarray
        ), f"The output should be np.ndarray but Serve got {type(output_arr)}."
        assert len(output_arr) == batch_size, (
            f"The output array should have shape of ({batch_size}, ...) "
            f"but Serve got {output_arr.shape}"
        )
        return [arr.squeeze(axis=0) for arr in np.split(output_arr, batch_size, axis=0)]

    return batched, unpack


@require_packages(["pandas"])
def collate_dataframe(
    input_list: List["pd.DataFrame"],
) -> Tuple["pd.DataFrame", Callable[["pd.DataFrame"], List["pd.DataFrame"]]]:
    import pandas as pd

    batch_size = len(input_list)
    batched = pd.concat(input_list, axis="index", ignore_index=True, copy=False)

    def unpack(output_df):
        if isinstance(output_df, list):
            return output_df
        assert isinstance(
            output_df, pd.DataFrame
        ), f"The output should be a DataFrame but Serve got {type(output_df)}"
        assert len(output_df) % batch_size == 0, (
            f"The output dataframe should have length divisible by {batch_size}, "
            f"but Serve got length {len(output_df)}."
        )
        return [df.reset_index(drop=True) for df in np.split(output_df, batch_size)]

    return batched, unpack


class ModelWrapper(SimpleSchemaIngress):
    """Serve any Ray AIR predictor from an AIR checkpoint.

    Args:
        predictor_cls(str, Type[Predictor]): The class or path for predictor class.
            The type must be a subclass of :class:`ray.ml.predictor.Predictor`.
        checkpoint(Checkpoint, str): The checkpoint object or an uri to load checkpoint
            from

            - The checkpoint object must be an instance of
              :class:`ray.ml.checkpoint.Checkpoint`.
            - The uri string will be called to construct a checkpoint object using
              ``Checkpoint.from_uri("uri_to_load_from")``.

        http_adapter(str, HTTPAdapterFn, None): The FastAPI input conversion
            function. By default, Serve will use the
            :ref:`NdArray <serve-ndarray-schema>` schema and convert to numpy array.
            You can pass in any FastAPI dependency resolver that returns
            an array. When you pass in a string, Serve will import it.
            Please refer to :ref:`Serve HTTP adatpers <serve-http-adapters>`
            documentation to learn more.
        batching_params(dict, None, False): override the default parameters to
            :func:`ray.serve.batch`. Pass ``False`` to disable batching.
        **predictor_kwargs: Additional keyword arguments passed to the
            ``Predictor.from_checkpoint()`` call.
    """

    def __init__(
        self,
        predictor_cls: Union[str, Type[Predictor]],
        checkpoint: Union[Checkpoint, str],
        http_adapter: Union[
            str, HTTPAdapterFn
        ] = "ray.serve.http_adapters.json_to_ndarray",
        batching_params: Optional[Union[Dict[str, int], bool]] = None,
        **predictor_kwargs,
    ):
        predictor_cls = _load_predictor_cls(predictor_cls)
        checkpoint = _load_checkpoint(checkpoint)

        self.model = predictor_cls.from_checkpoint(checkpoint, **predictor_kwargs)

        # Configure Batching
        if batching_params is False:

            async def predict_impl(inp: Union[np.ndarray, "pd.DataFrame"]):
                out = self.model.predict(inp)
                if isinstance(out, ray.ObjectRef):
                    out = await out
                return out

        else:
            batching_params = batching_params or dict()

            @serve.batch(**batching_params)
            async def predict_impl(inp: Union[List[np.ndarray], List["pd.DataFrame"]]):

                if isinstance(inp[0], np.ndarray):
                    collate_func = collate_array
                elif pd is not None and isinstance(inp[0], pd.DataFrame):
                    collate_func = collate_dataframe
                else:
                    raise ValueError(
                        "ModelWrapper only accepts numpy array or dataframe as input "
                        f"but got types {[type(i) for i in inp]}"
                    )

                batched, unpack = collate_func(inp)
                out = self.model.predict(batched)
                if isinstance(out, ray.ObjectRef):
                    out = await out
                return unpack(out)

        self.predict_impl = predict_impl

        super().__init__(http_adapter)

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.predict_impl(inp)


@serve.deployment
class ModelWrapperDeployment(ModelWrapper):
    """Ray Serve Deployment of the ModelWrapper class."""
