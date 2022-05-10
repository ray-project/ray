from typing import Dict, Optional, Type, Union

from ray._private.utils import import_attr
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor
from ray.serve.drivers import HTTPAdapterFn, SimpleSchemaIngress
import ray
from ray import serve


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


class ModelWrapper(SimpleSchemaIngress):
    """Serve any Ray AIR predictor from an AIR checkpoint.

    Args:
        predictor_cls(str, Type[Predictor]): The class or path for predictor class.
            The type must be a subclass of :class:`ray.ml.predicotr.Predictor`.
        checkpoint(Checkpoint, dict): The checkpoint object or a dictionary describe
            the object.

            - The checkpoint object must be a subclass of
              :class:`ray.ml.checkpoint.Checkpoint`.
            - The dictionary should be in the form of
              ``{"checkpoint_cls": "import.path.MyCheckpoint",
              "uri": "uri_to_load_from"}``.
              Serve will then call ``MyCheckpoint.from_uri("uri_to_load_from")`` to
              instantiate the object.

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
        checkpoint: Union[Checkpoint, Dict],
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

        super().__init__(http_adapter)

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.batched_predict(inp)


@serve.deployment
class ModelWrapperDeployment(ModelWrapper):
    """Ray Serve Deployment of the ModelWrapper class."""
